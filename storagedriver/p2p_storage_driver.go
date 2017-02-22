package storagedriver

import (
	"io"
	"regexp"
	"time"

	"golang.org/x/time/rate"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/configuration"
	"code.uber.internal/infra/kraken/storage"
	"code.uber.internal/infra/kraken/tracker"

	"path/filepath"

	"fmt"

	"os"

	"strings"

	cache "code.uber.internal/infra/dockermover/storage"
	"github.com/anacrolix/torrent"
	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/garyburd/redigo/redis"
)

// The path layout in the storage backend is roughly as follows:
//
//		<root>/v2
//			-> repositories/
// 				-><name>/
// 					-> _manifests/
// 						revisions
//							-> <manifest digest path>
//								-> link
// 						tags/<tag>
//							-> current/link
// 							-> index
//								-> <algorithm>/<hex digest>/link
// 					-> _layers/
// 						<layer links to blob store>
// 					-> _uploads/<id>
// 						data
// 						startedat
// 						hashstates/<algorithm>/<offset>
//			-> blob/<algorithm>
//				<split directory content addressable storage>
//

const (
	// Name of storage driver
	Name         = "p2p"
	retries      = 3
	p2ptimeout   = 120     //seconds
	readtimeout  = 15 * 60 //seconds
	writetimeout = 15 * 60 //seconds
)

func init() {
	factory.Register(Name, &p2pStorageDriverFactory{})
}

type p2pStorageDriverFactory struct{}

func (factory *p2pStorageDriverFactory) Create(params map[string]interface{}) (storagedriver.StorageDriver, error) {
	configFile, ok := params["config"]
	if !ok {
		log.Fatal("No config file specified")
	}
	// load config
	cp := configuration.GetConfigFilePath(configFile.(string))
	config := configuration.NewConfig(cp)

	// init temp dir
	os.RemoveAll(config.PushTempDir)
	err := os.MkdirAll(config.PushTempDir, 0755)
	if err != nil {
		return nil, err
	}

	// init cache dir
	err = os.MkdirAll(config.CacheDir, 0755)
	if err != nil {
		return nil, err
	}

	// init new cache
	// the storage driver and p2p client storage share the same lru
	l, err := cache.NewFileCacheMap(config.CacheMapSize, config.CacheSize)
	if err != nil {
		return nil, err
	}

	// init storage
	p2pStorage, err := storage.NewManager(config, l)
	if err != nil {
		log.Fatal(err.Error())
	}

	// init client
	p2pClient, err := torrent.NewClient(&torrent.Config{
		DefaultStorage:      p2pStorage,
		NoUpload:            false,
		Seed:                true,
		ListenAddr:          config.ClientAddr,
		NoDHT:               true,
		Debug:               true,
		DisableTCP:          false,
		DownloadRateLimiter: rate.NewLimiter(rate.Inf, 1),
		UploadRateLimiter:   rate.NewLimiter(rate.Inf, 1),
		DisableEncryption:   true,
		ForceEncryption:     false,
		PreferNoEncryption:  true,
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	// load storage files from disk
	p2pStorage.LoadFromDisk(p2pClient)

	// init redis connection
	redisConn, err := redis.DialURL(config.RedisURL)
	if err != nil {
		log.Fatal(err.Error())
	}

	t := tracker.NewTracker(config, redisConn)
	go t.Serve()

	createTest := params["createTest"]
	if createTest.(bool) {
		key, ok := params["testKey"]
		if !ok || key == "" {
			log.Fatal("Test layer key not specified")
		}
		path, ok := params["testPath"]
		if !ok || path == "" {
			log.Fatal("Test layer path not specified")
		}
		err := t.CreateTorrent(key.(string), path.(string))
		if err != nil {
			log.Error(err.Error())
		}
	}

	return NewP2PStorageDriver(config, p2pClient, l, t), nil
}

// P2PStorageDriver is a storage driver
type P2PStorageDriver struct {
	config     *configuration.Config
	p2pClient  *torrent.Client
	p2pTracker *tracker.Tracker
	lru        *cache.FileCacheMap
	blobs      *Blobs
	uploads    *Uploads
	hashstates *HashStates
}

// NewP2PStorageDriver creates a new P2PStorageDriver given Manager
func NewP2PStorageDriver(c *configuration.Config, cl *torrent.Client, l *cache.FileCacheMap, t *tracker.Tracker) *P2PStorageDriver {
	return &P2PStorageDriver{
		config:     c,
		p2pClient:  cl,
		lru:        l,
		p2pTracker: t,
		blobs:      NewBlobs(t, cl, l),
		uploads:    NewUploads(t, l),
		hashstates: NewHashStates(),
	}
}

// Name returns driver namae
func (d *P2PStorageDriver) Name() string {
	return Name
}

// GetContent returns content in the path
// sample path: /docker/registry/v2/repositories/external/ubuntu/_layers/sha256/a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4/link
func (d *P2PStorageDriver) GetContent(ctx context.Context, path string) (data []byte, err error) {
	log.Infof("GetContent %s", path)
	ts := strings.Split(path, "/")
	contentType := ts[len(ts)-1]
	sha := filepath.Base(filepath.Dir(path))
	switch contentType {
	case "link":
		isTag, tagOrDigest, err := d.isTag(path)
		if err != nil {
			return nil, err
		}
		repo, err := d.getRepoName(path)
		if err != nil {
			return nil, err
		}
		if isTag {
			sha, err = d.p2pTracker.GetDigestFromRepoTag(repo, tagOrDigest)
			if err != nil {
				return nil, err
			}
		}
		return []byte("sha256:" + sha), nil
	case "startedat":
		uuid := ts[len(ts)-2]
		return d.uploads.getUploadStartTime(d.config.PushTempDir, uuid)
	case "data":
		// get or download content
		return d.blobs.getOrDownloadBlobData(path, sha)
	default:
		if len(ts) > 3 && ts[len(ts)-3] == "hashstates" {
			uuid := ts[len(ts)-4]
			alg := ts[len(ts)-2]
			code := ts[len(ts)-1]
			return d.hashstates.getHashState(d.config.PushTempDir, uuid, alg, code)
		}
		return nil, fmt.Errorf("Invalid request %s", path)
	}
}

// Reader returns a reader of path at offset
func (d *P2PStorageDriver) Reader(ctx context.Context, path string, offset int64) (reader io.ReadCloser, err error) {
	log.Infof("Reader %s", path)
	ts := strings.Split(path, "/")
	if len(ts) < 3 {
		return nil, fmt.Errorf("Invalid request %s", path)
	}
	fileType := ts[len(ts)-3]
	switch fileType {
	case "_uploads":
		uuid := ts[len(ts)-2]
		return d.uploads.getUploadReader(path, d.config.PushTempDir, uuid, offset)
	default:
		sha := ts[len(ts)-2]
		return d.blobs.getOrDownloadBlobReader(path, sha, offset)
	}
}

// PutContent writes content to path
func (d *P2PStorageDriver) PutContent(ctx context.Context, path string, content []byte) error {
	log.Infof("PutContent %s", path)
	ts := strings.Split(path, "/")
	contentType := ts[len(ts)-1]
	switch contentType {
	case "startedat":
		uuid := ts[len(ts)-2]
		return d.uploads.initUpload(d.config.PushTempDir, uuid)
	case "data":
		sha := ts[len(ts)-2]
		return d.blobs.putBlobData(path, d.config.CacheDir, sha, content)
	case "link":
		if len(ts) < 7 {
			return nil
		}

		if ts[len(ts)-7] == "_manifests" {
			repo, err := d.getRepoName(path)
			if err != nil {
				return err
			}
			digest := ts[len(ts)-2]
			tag := ts[len(ts)-5]
			if err = d.p2pTracker.SetDigestForRepoTag(repo, tag, digest); err != nil {
				return err
			}
		}

		return nil
	default:
		if len(ts) > 3 && ts[len(ts)-3] == "hashstates" {
			uuid := ts[len(ts)-4]
			alg := ts[len(ts)-2]
			code := ts[len(ts)-1]
			_, err := d.hashstates.putHashState(d.config.PushTempDir, uuid, alg, code, content)
			return err
		}
		return fmt.Errorf("Invalid request %s", path)
	}
}

// Writer returns a writer of path
func (d *P2PStorageDriver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	log.Infof("Writer %s", path)

	contentType := filepath.Base(path)
	uuid := filepath.Base(filepath.Dir(path))
	switch contentType {
	case "data":
		break
	default:
		return nil, fmt.Errorf("Invalid request %s", path)
	}

	var fw storagedriver.FileWriter
	var err error
	to := make(chan byte, 1)
	go func() {
		time.Sleep(writetimeout * time.Second)
		to <- uint8(1)
	}()

	fw, err = NewChanWriteCloser(d.config.PushTempDir+uuid, append)

	go func() {
		// wait for file close or timeout
		select {
		case res := <-fw.(*ChanWriteCloser).Chan:
			switch res {
			// cancel
			case uint8(0):
				log.Infof("Cancel writting file %s", path)
				break
			// commit
			case uint8(1):
			}
			break
		case <-to:
			log.Debugf("Timeout writting file %s", path)
			// cancel write if timeout
			fw.Cancel()
		}
	}()

	return fw, err
}

// Stat returns fileinfo of path
func (d *P2PStorageDriver) Stat(ctx context.Context, path string) (fi storagedriver.FileInfo, err error) {
	log.Infof("Stat %s", path)
	st := strings.Split(path, "/")
	if len(st) < 3 {
		return nil, fmt.Errorf("Invalid request %s", path)
	}
	fileType := st[len(st)-3]
	switch fileType {
	case "_uploads":
		uuid := st[len(st)-2]
		return d.uploads.getUploadDataStat(d.config.PushTempDir, uuid)
	default:
		sha := st[len(st)-2]
		return d.blobs.getBlobStat(path, sha)
	}
}

// List returns a list of contant given path
func (d *P2PStorageDriver) List(ctx context.Context, path string) ([]string, error) {
	log.Infof("List %s", path)
	st := strings.Split(path, "/")
	if len(st) < 2 {
		return nil, fmt.Errorf("Invalid request %s", path)
	}
	contentType := st[len(st)-2]
	switch contentType {
	case "hashstates":
		uuid := st[len(st)-3]
		s, err := d.hashstates.listHashStates(d.config.PushTempDir, uuid, path)
		return s, err
	default:
		break
	}
	return nil, fmt.Errorf("Not implemented.")
}

// Move moves sourcePath to destPath
func (d *P2PStorageDriver) Move(ctx context.Context, sourcePath string, destPath string) (err error) {
	log.Infof("Move %s %s", sourcePath, destPath)
	srcst := strings.Split(sourcePath, "/")
	if len(srcst) < 3 {
		return fmt.Errorf("Invalid request %s", sourcePath)
	}
	srcFileType := srcst[len(srcst)-3]
	srcsha := srcst[len(srcst)-2]

	destst := strings.Split(destPath, "/")
	if len(srcst) < 3 {
		return fmt.Errorf("Invalid request %s", destPath)
	}
	destsha := destst[len(destst)-2]
	switch srcFileType {
	case "_uploads":
		return d.uploads.commitUpload(d.config.PushTempDir, srcsha, d.config.CacheDir, destsha)
	default:
		return fmt.Errorf("Not implemented.")
	}
}

// Delete deletes path
func (d *P2PStorageDriver) Delete(ctx context.Context, path string) error {
	log.Infof("Delete %s", path)
	return storagedriver.PathNotFoundError{
		DriverName: "p2p",
		Path:       path,
	}
}

// URLFor returns url for path
func (d *P2PStorageDriver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	log.Infof("URLFor %s", path)
	return "", fmt.Errorf("Not implemented.")
}

// isTag return if path contains tag and returns the tag, or digest if it contains digest instead
func (d *P2PStorageDriver) isTag(path string) (bool, string, error) {
	tagRegex := regexp.MustCompile(".*_manifests/tags/.*/current/link")
	digestRegex := regexp.MustCompile(".*/sha256/.*/link")
	st := strings.Split(path, "/")
	if tagRegex.MatchString(path) {
		if len(st) < 3 {
			return false, "", fmt.Errorf("Invalid path format %s", path)
		}
		return true, st[len(st)-3], nil
	}
	if digestRegex.MatchString(path) {
		if len(st) < 2 {
			return false, "", fmt.Errorf("Invalid path format %s", path)
		}
		return false, st[len(st)-2], nil
	}
	return false, "", fmt.Errorf("Invalid path format %s", path)
}

// getRepoName returns the repo name given path
func (d *P2PStorageDriver) getRepoName(path string) (string, error) {
	prefix := regexp.MustCompile("^.*repositories/")
	suffix := regexp.MustCompile("(/_layer|/_manifest|/_uploads).*")
	name := suffix.ReplaceAllString(prefix.ReplaceAllString(path, ""), "")
	if name == "" {
		return "", fmt.Errorf("Error getting repo name %s", path)
	}
	return name, nil
}
