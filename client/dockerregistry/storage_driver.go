package dockerregistry

import (
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrent"

	"code.uber.internal/go-common.git/x/log"
	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/robfig/cron"
	"github.com/uber-go/tally"
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
	Name            = "kraken"
	retries         = 3
	downloadTimeout = 120     //seconds
	readtimeout     = 15 * 60 //seconds
	writetimeout    = 15 * 60 //seconds
)

func init() {
	factory.Register(Name, &krakenStorageDriverFactory{})
}

type krakenStorageDriverFactory struct{}

func (factory *krakenStorageDriverFactory) Create(params map[string]interface{}) (storagedriver.StorageDriver, error) {
	configParam, ok := params["config"]
	if !ok || configParam == nil {
		log.Fatal("Failed to create storage driver. No configuration initiated.")
	}
	config := configParam.(*Config)

	storeParam, ok := params["store"]
	if !ok || storeParam == nil {
		log.Fatal("Failed to create storage driver. No file store initiated.")
	}
	store := storeParam.(*store.LocalStore)

	clientParam, ok := params["torrentclient"]
	if !ok || clientParam == nil {
		log.Fatal("Failed to create storage driver. No torrent agent initated.")
	}
	client := clientParam.(torrent.Client)

	metricsParam, ok := params["metrics"]
	if !ok || metricsParam == nil {
		log.Fatal("Failed to create storage driver. No metrics initiated.")
	}
	metrics := metricsParam.(tally.Scope)

	sd, err := NewKrakenStorageDriver(config, store, client, metrics)
	if err != nil {
		return nil, err
	}

	return sd, nil
}

// KrakenStorageDriver is a storage driver
type KrakenStorageDriver struct {
	config  *Config
	tcl     torrent.Client
	store   *store.LocalStore
	blobs   *Blobs
	uploads *Uploads
	tags    Tags
	metrics tally.Scope
}

// NewKrakenStorageDriver creates a new KrakenStorageDriver given Manager
func NewKrakenStorageDriver(
	c *Config,
	s *store.LocalStore,
	cl torrent.Client,
	metrics tally.Scope) (*KrakenStorageDriver, error) {
	tags, err := NewDockerTags(c, s, cl, metrics)
	if err != nil {
		return nil, err
	}

	// Start a cron to delete expired tags.
	if c.TagDeletion.Enable && c.TagDeletion.Interval > 0 {
		log.Info("Scheduling tag cleanup cron")
		deleteExpiredTagsCron := cron.New()
		interval := fmt.Sprintf("@every %ds", c.TagDeletion.Interval)
		err = deleteExpiredTagsCron.AddFunc(interval, func() {
			log.Info("Running tag cleanup cron")
			tags.DeleteExpiredTags(c.TagDeletion.RetentionCount,
				time.Now().Add(time.Duration(-c.TagDeletion.RetentionTime)*time.Second))
		})
		if err != nil {
			return nil, err
		}
		deleteExpiredTagsCron.Start()
	}

	return &KrakenStorageDriver{
		config:  c,
		tcl:     cl,
		store:   s,
		blobs:   NewBlobs(cl, s),
		uploads: NewUploads(cl, s),
		tags:    tags,
		metrics: metrics,
	}, nil
}

// Name returns driver namae
func (d *KrakenStorageDriver) Name() string {
	return Name
}

// GetContent returns content in the path
// sample path: /docker/registry/v2/repositories/external/ubuntu/_layers/sha256/a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4/link
func (d *KrakenStorageDriver) GetContent(ctx context.Context, path string) (data []byte, err error) {
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
			sha, err = d.tags.GetTag(repo, tagOrDigest)
			if err != nil {
				return nil, err
			}
		}
		return []byte("sha256:" + sha), nil
	case "startedat":
		uuid := ts[len(ts)-2]
		return d.uploads.getUploadStartTime(d.store.Config().UploadDir, uuid)
	case "data":
		// get or download content
		return d.blobs.getOrDownloadBlobData(sha)
	default:
		if len(ts) > 3 && ts[len(ts)-3] == "hashstates" {
			uuid := ts[len(ts)-4]
			algo := ts[len(ts)-2]
			offset := ts[len(ts)-1]
			return d.store.GetUploadFileHashState(uuid, algo, offset)
		}
		return nil, fmt.Errorf("Invalid request %s", path)
	}
}

// Reader returns a reader of path at offset
func (d *KrakenStorageDriver) Reader(ctx context.Context, path string, offset int64) (reader io.ReadCloser, err error) {
	log.Infof("Reader %s", path)
	ts := strings.Split(path, "/")
	if len(ts) < 3 {
		return nil, fmt.Errorf("Invalid request %s", path)
	}
	fileType := ts[len(ts)-3]
	switch fileType {
	case "_uploads":
		uuid := ts[len(ts)-2]
		return d.uploads.getUploadReader(uuid, offset)
	default:
		sha := ts[len(ts)-2]
		return d.blobs.getOrDownloadBlobReader(sha, offset)
	}
}

// PutContent writes content to path
func (d *KrakenStorageDriver) PutContent(ctx context.Context, path string, content []byte) error {
	log.Infof("PutContent %s", path)
	ts := strings.Split(path, "/")
	contentType := ts[len(ts)-1]
	switch contentType {
	case "startedat":
		uuid := ts[len(ts)-2]
		return d.uploads.initUpload(uuid)
	case "data":
		sha := ts[len(ts)-2]
		return d.uploads.putBlobData(sha, content)
	case "link":
		if len(ts) < 7 {
			return nil
		}

		// This check filters out manifestRevisionLinkPathSpec and manifestTagCurrentPathSpec
		if ts[len(ts)-7] == "_manifests" {
			repo, err := d.getRepoName(path)
			if err != nil {
				log.Errorf("PutContent: cannot get repo %s", path)
				return err
			}
			digest := ts[len(ts)-2]
			tag := ts[len(ts)-5]
			err = d.tags.CreateTag(repo, tag, digest)
			if err != nil {
				log.Errorf("PutContent: cannot create tag %s:%s", repo, path)
				return err
			}
		}
		return nil
	default:
		if len(ts) > 3 && ts[len(ts)-3] == "hashstates" {
			uuid := ts[len(ts)-4]
			algo := ts[len(ts)-2]
			offset := ts[len(ts)-1]
			err := d.store.SetUploadFileHashState(uuid, content, algo, offset)
			return err
		}
		return fmt.Errorf("Invalid request %s", path)
	}
}

// Writer returns a writer of path
func (d *KrakenStorageDriver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
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
func (d *KrakenStorageDriver) Stat(ctx context.Context, path string) (fi storagedriver.FileInfo, err error) {
	log.Infof("Stat %s", path)
	st := strings.Split(path, "/")
	if len(st) < 3 {
		return nil, fmt.Errorf("Invalid request %s", path)
	}
	fileType := st[len(st)-3]
	switch fileType {
	case "_uploads":
		uuid := st[len(st)-2]
		return d.uploads.getUploadDataStat(d.store.Config().UploadDir, uuid)
	default:
		sha := st[len(st)-2]
		return d.blobs.getBlobStat(sha)
	}
}

// List returns a list of content given path
func (d *KrakenStorageDriver) List(ctx context.Context, path string) ([]string, error) {
	log.Infof("List %s", path)
	st := strings.Split(path, "/")
	if len(st) < 2 {
		return nil, fmt.Errorf("Invalid request %s", path)
	}
	contentType := st[len(st)-2]
	switch contentType {
	case "hashstates":
		uuid := st[len(st)-3]
		s, err := d.store.ListUploadFileHashStatePaths(uuid)
		return s, err
	case "_manifests":
		repo, err := d.getRepoName(path)
		if err != nil {
			return nil, err
		}
		if st[len(st)-1] == "tags" {
			return d.tags.ListTags(repo)
		}
	default:
		break
	}
	return nil, fmt.Errorf("Not implemented")
}

// Move moves sourcePath to destPath
func (d *KrakenStorageDriver) Move(ctx context.Context, sourcePath string, destPath string) (err error) {
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
		return d.uploads.commitUpload(srcsha, d.store.Config().CacheDir, destsha)
	default:
		return fmt.Errorf("Not implemented")
	}
}

// Delete deletes path
func (d *KrakenStorageDriver) Delete(ctx context.Context, path string) error {
	log.Infof("Delete %s", path)
	return storagedriver.PathNotFoundError{
		DriverName: "p2p",
		Path:       path,
	}
}

// URLFor returns url for path
func (d *KrakenStorageDriver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	log.Infof("URLFor %s", path)
	return "", fmt.Errorf("Not implemented")
}

// isTag return if path contains tag and returns the tag, or digest if it contains digest instead
func (d *KrakenStorageDriver) isTag(path string) (bool, string, error) {
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
func (d *KrakenStorageDriver) getRepoName(path string) (string, error) {
	prefix := regexp.MustCompile("^.*repositories/")
	suffix := regexp.MustCompile("(/_layer|/_manifests|/_uploads).*")
	name := suffix.ReplaceAllString(prefix.ReplaceAllString(path, ""), "")
	if name == "" {
		return "", fmt.Errorf("Error getting repo name %s", path)
	}
	return name, nil
}
