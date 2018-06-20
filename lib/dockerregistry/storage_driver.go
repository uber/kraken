package dockerregistry

import (
	"fmt"
	"io"

	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
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

func getParam(params map[string]interface{}, name string) interface{} {
	p, ok := params[name]
	if !ok || p == nil {
		log.Fatalf("Required parameter %s not found", name)
	}
	return p
}

func (factory *krakenStorageDriverFactory) Create(
	params map[string]interface{}) (storagedriver.StorageDriver, error) {

	// Common parameters.
	component := getParam(params, "component").(string)
	config := getParam(params, "config").(Config)
	transferer := getParam(params, "transferer").(transfer.ImageTransferer)
	metrics := getParam(params, "metrics").(tally.Scope)

	switch component {
	case _proxy:
		castore := getParam(params, "castore").(*store.CAStore)
		return NewReadWriteStorageDriver(config, castore, transferer, metrics), nil
	case _agent:
		blobstore := getParam(params, "blobstore").(BlobStore)
		return NewReadOnlyStorageDriver(config, blobstore, transferer, metrics), nil
	default:
		return nil, fmt.Errorf("unknown component %s", component)
	}
}

// KrakenStorageDriver is a storage driver
type KrakenStorageDriver struct {
	config     Config
	transferer transfer.ImageTransferer
	blobs      *blobs
	uploads    uploads
	manifests  *manifests
	metrics    tally.Scope
}

// NewReadWriteStorageDriver creates a KrakenStorageDriver which can push / pull blobs.
func NewReadWriteStorageDriver(
	config Config,
	cas *store.CAStore,
	transferer transfer.ImageTransferer,
	metrics tally.Scope) *KrakenStorageDriver {

	return &KrakenStorageDriver{
		config:     config,
		transferer: transferer,
		blobs:      newBlobs(cas, transferer),
		uploads:    newCASUploads(cas, transferer),
		manifests:  newManifests(transferer),
		metrics:    metrics,
	}
}

// NewReadOnlyStorageDriver creates a KrakenStorageDriver which can only pull blobs.
func NewReadOnlyStorageDriver(
	config Config,
	bs BlobStore,
	transferer transfer.ImageTransferer,
	metrics tally.Scope) *KrakenStorageDriver {

	return &KrakenStorageDriver{
		config:     config,
		transferer: transferer,
		blobs:      newBlobs(bs, transferer),
		uploads:    disabledUploads{},
		manifests:  newManifests(transferer),
		metrics:    metrics,
	}
}

// Name returns driver namae
func (d *KrakenStorageDriver) Name() string {
	return Name
}

// GetContent returns content in the path
// sample path: /docker/registry/v2/repositories/external/ubuntu/_layers/sha256/a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4/link
func (d *KrakenStorageDriver) GetContent(ctx context.Context, path string) (data []byte, err error) {
	log.Debugf("(*KrakenStorageDriver).GetContent %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	switch pathType {
	case _manifests:
		return d.manifests.getDigest(path, pathSubType)
	case _uploads:
		return d.uploads.getContent(path, pathSubType)
	case _layers:
		return d.blobs.getDigest(path)
	case _blobs:
		return d.blobs.getContent(path)
	}
	return nil, InvalidRequestError{path}
}

// Reader returns a reader of path at offset
func (d *KrakenStorageDriver) Reader(ctx context.Context, path string, offset int64) (reader io.ReadCloser, err error) {
	log.Debugf("(*KrakenStorageDriver).Reader %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	switch pathType {
	case _uploads:
		return d.uploads.reader(path, pathSubType, offset)
	case _blobs:
		return d.blobs.reader(path, offset)
	default:
		return nil, InvalidRequestError{path}
	}
}

// PutContent writes content to path
func (d *KrakenStorageDriver) PutContent(ctx context.Context, path string, content []byte) error {
	log.Debugf("(*KrakenStorageDriver).PutContent %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return err
	}

	switch pathType {
	case _manifests:
		return d.manifests.putContent(path, pathSubType)
	case _uploads:
		return d.uploads.putContent(path, pathSubType, content)
	case _layers:
		// noop
		return nil
	case _blobs:
		return d.uploads.putBlobContent(path, content)
	default:
		return InvalidRequestError{path}
	}
}

// Writer returns a writer of path
func (d *KrakenStorageDriver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	log.Debugf("(*KrakenStorageDriver).Writer %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	switch pathType {
	case _uploads:
		w, err := d.uploads.writer(path, pathSubType)
		if err != nil {
			return nil, err
		}
		if append {
			if _, err := w.Seek(0, io.SeekEnd); err != nil {
				return nil, err
			}
		}
		return w, nil
	default:
		return nil, InvalidRequestError{path}
	}
}

// Stat returns fileinfo of path
func (d *KrakenStorageDriver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	log.Debugf("(*KrakenStorageDriver).Stat %s", path)
	pathType, _, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	switch pathType {
	case _uploads:
		return d.uploads.stat(path)
	case _blobs:
		return d.blobs.stat(path)
	default:
		return nil, InvalidRequestError{path}
	}
}

// List returns a list of content given path
func (d *KrakenStorageDriver) List(ctx context.Context, path string) ([]string, error) {
	log.Debugf("(*KrakenStorageDriver).List %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	switch pathType {
	case _uploads:
		return d.uploads.list(path, pathSubType)
	case _manifests:
		return d.manifests.list(path, pathSubType)
	default:
		return nil, InvalidRequestError{path}
	}
}

// Move moves sourcePath to destPath
func (d *KrakenStorageDriver) Move(ctx context.Context, sourcePath string, destPath string) error {
	log.Debugf("(*KrakenStorageDriver).Move %s %s", sourcePath, destPath)
	pathType, _, err := ParsePath(sourcePath)
	if err != nil {
		return err
	}

	switch pathType {
	case _uploads:
		return d.uploads.move(sourcePath, destPath)
	default:
		return InvalidRequestError{sourcePath + " to " + destPath}
	}
}

// Delete deletes path
func (d *KrakenStorageDriver) Delete(ctx context.Context, path string) error {
	log.Debugf("(*KrakenStorageDriver).Delete %s", path)
	return storagedriver.PathNotFoundError{
		DriverName: "p2p",
		Path:       path,
	}
}

// URLFor returns url for path
func (d *KrakenStorageDriver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	log.Debugf("(*KrakenStorageDriver).URLFor %s", path)
	return "", fmt.Errorf("Not implemented")
}
