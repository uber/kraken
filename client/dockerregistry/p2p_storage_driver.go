package dockerregistry

import (
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"
	"code.uber.internal/infra/kraken/kraken/test-tracker"

	"code.uber.internal/go-common.git/x/log"
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
	configParam, ok := params["config"]
	if !ok || configParam == nil {
		log.Fatal("Failed to create storage driver. No configuration initiated.")
	}
	config := configParam.(*configuration.Config)

	clientParam, ok := params["torrent-client"]
	if !ok || clientParam == nil {
		log.Fatal("Failed to create storage driver. No torrent agnet initated.")
	}
	client := clientParam.(*torrent.Client)

	storeParam, ok := params["store"]
	if !ok || storeParam == nil {
		log.Fatal("Failed to create storage driver. No file store initiated.")
	}
	store := storeParam.(*store.LocalFileStore)

	// init redis connection pools
	pool := &redis.Pool{
		MaxIdle:     3,
		MaxActive:   6,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.DialURL(config.RedisURL) },
	}

	t := tracker.NewTracker(config, pool)
	go t.Serve()

	return NewP2PStorageDriver(config, client, store, t), nil
}

// P2PStorageDriver is a storage driver
type P2PStorageDriver struct {
	config     *configuration.Config
	p2pClient  *torrent.Client
	p2pTracker *tracker.Tracker
	store      *store.LocalFileStore
	blobs      *Blobs
	uploads    *Uploads
	hashstates *HashStates
}

// NewP2PStorageDriver creates a new P2PStorageDriver given Manager
func NewP2PStorageDriver(c *configuration.Config, cl *torrent.Client, s *store.LocalFileStore, t *tracker.Tracker) *P2PStorageDriver {
	return &P2PStorageDriver{
		config:     c,
		p2pClient:  cl,
		store:      s,
		p2pTracker: t,
		blobs:      NewBlobs(t, cl, s),
		uploads:    NewUploads(t, cl, s),
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
		return d.blobs.getOrDownloadBlobData(sha)
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
		return d.blobs.getOrDownloadBlobReader(sha, offset)
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
		return d.uploads.putBlobData(sha, content)
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

	return d.store.GetUploadFileReadWriter(uuid)
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
		return d.blobs.getBlobStat(sha)
	}
}

// List returns a list of content given path
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
