package dockerregistry

import (
	"fmt"
	"io"
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
//			-> blobs/<algorithm>
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

// InvalidRequestError implements error and contains the path that is not supported
type InvalidRequestError struct {
	path string
}

func (e InvalidRequestError) Error() string {
	return fmt.Sprintf("invalid request: %s", e.path)
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
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	switch pathType {
	case _manifests:
		return d.tags.GetContent(path, pathSubType)
	case _uploads:
		return d.uploads.GetContent(path, pathSubType)
	case _layers:
		return d.blobs.GetDigest(path)
	case _blobs:
		return d.blobs.GetContent(path)
	}
	return nil, InvalidRequestError{path}
}

// Reader returns a reader of path at offset
func (d *KrakenStorageDriver) Reader(ctx context.Context, path string, offset int64) (reader io.ReadCloser, err error) {
	log.Infof("Reader %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	switch pathType {
	case _uploads:
		return d.uploads.GetReader(path, pathSubType, offset)
	case _blobs:
		return d.blobs.GetReader(path, offset)
	default:
		return nil, InvalidRequestError{path}
	}
}

// PutContent writes content to path
func (d *KrakenStorageDriver) PutContent(ctx context.Context, path string, content []byte) error {
	log.Infof("PutContent %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return err
	}

	switch pathType {
	case _manifests:
		return d.tags.PutContent(path, pathSubType)
	case _uploads:
		return d.uploads.PutUploadContent(path, pathSubType, content)
	case _layers:
		// noop
		return nil
	case _blobs:
		return d.uploads.PutBlobContent(path, content)
	default:
		return InvalidRequestError{path}
	}
}

// Writer returns a writer of path
func (d *KrakenStorageDriver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	log.Infof("Writer %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	switch pathType {
	case _uploads:
		return d.uploads.GetWriter(path, pathSubType)
	default:
		return nil, InvalidRequestError{path}
	}
}

// Stat returns fileinfo of path
func (d *KrakenStorageDriver) Stat(ctx context.Context, path string) (fi storagedriver.FileInfo, err error) {
	log.Infof("Stat %s", path)
	pathType, _, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	switch pathType {
	case _uploads:
		return d.uploads.GetStat(path)
	case _blobs:
		return d.blobs.GetStat(path)
	default:
		return nil, InvalidRequestError{path}
	}
}

// List returns a list of content given path
func (d *KrakenStorageDriver) List(ctx context.Context, path string) ([]string, error) {
	log.Infof("List %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	switch pathType {
	case _uploads:
		return d.uploads.ListHashStates(path, pathSubType)
	case _manifests:
		return d.tags.ListManifests(path, pathSubType)
	default:
		return nil, InvalidRequestError{path}
	}
}

// Move moves sourcePath to destPath
func (d *KrakenStorageDriver) Move(ctx context.Context, sourcePath string, destPath string) error {
	log.Infof("Move %s %s", sourcePath, destPath)
	pathType, _, err := ParsePath(sourcePath)
	if err != nil {
		return err
	}

	switch pathType {
	case _uploads:
		return d.uploads.Move(sourcePath, destPath)
	default:
		return InvalidRequestError{sourcePath + " to " + destPath}
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
