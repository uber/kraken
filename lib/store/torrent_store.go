package store

import (
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/lib/store/base"
	"code.uber.internal/infra/kraken/lib/store/metadata"
	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
)

// TorrentStore allows simultaneously downloading and uploading files.
type TorrentStore struct {
	backend       base.FileStore
	downloadState base.FileState
	cacheState    base.FileState
	cleanup       *cleanupManager
}

// NewTorrentStore creates a new TorrentStore.
func NewTorrentStore(config TorrentStoreConfig, stats tally.Scope) (*TorrentStore, error) {
	stats = stats.Tagged(map[string]string{
		"module": "torrentstore",
	})

	for _, dir := range []string{config.DownloadDir, config.CacheDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %s", dir, err)
		}
	}

	backend, err := base.NewCASFileStore(clock.New())
	if err != nil {
		return nil, fmt.Errorf("new base store: %s", err)
	}
	downloadState := base.NewFileState(config.DownloadDir)
	cacheState := base.NewFileState(config.CacheDir)

	cleanup, err := newCleanupManager(clock.New(), stats)
	if err != nil {
		return nil, fmt.Errorf("new cleanup manager: %s", err)
	}
	cleanup.addJob(
		"download",
		config.DownloadCleanup,
		backend.NewFileOp().AcceptState(downloadState))
	cleanup.addJob(
		"cache",
		config.CacheCleanup,
		backend.NewFileOp().AcceptState(cacheState))

	return &TorrentStore{
		backend:       backend,
		downloadState: downloadState,
		cacheState:    cacheState,
		cleanup:       cleanup,
	}, nil
}

// CreateDownloadFile creates an empty download file initialized with length.
func (s *TorrentStore) CreateDownloadFile(name string, length int64) error {
	return s.backend.NewFileOp().CreateFile(name, s.downloadState, length)
}

// GetDownloadFileReadWriter returns a FileReadWriter for name.
func (s *TorrentStore) GetDownloadFileReadWriter(name string) (FileReadWriter, error) {
	return s.backend.NewFileOp().AcceptState(s.downloadState).GetFileReadWriter(name)
}

// MoveDownloadFileToCache moves a download file to the cache.
func (s *TorrentStore) MoveDownloadFileToCache(name string) error {
	return s.backend.NewFileOp().AcceptState(s.downloadState).MoveFile(name, s.cacheState)
}

// InCacheError returns true for errors originating from file store operations
// which do not accept files in cache state.
func (s *TorrentStore) InCacheError(err error) bool {
	fse, ok := err.(*base.FileStateError)
	return ok && fse.State == s.cacheState
}

// InDownloadError returns true for errors originating from file store operations
// which do not accept files in download state.
func (s *TorrentStore) InDownloadError(err error) bool {
	fse, ok := err.(*base.FileStateError)
	return ok && fse.State == s.downloadState
}

// TorrentStoreStateAcceptor is a builder which allows TorrentStore clients to specify which
// states an operation may be accepted within. Should only be used for read / write
// operations which are acceptable in any state.
type TorrentStoreStateAcceptor struct {
	store *TorrentStore
	op    base.FileOp
}

// States returns a new TorrentStoreStateAcceptor builder.
func (s *TorrentStore) States() *TorrentStoreStateAcceptor {
	return &TorrentStoreStateAcceptor{
		store: s,
		op:    s.backend.NewFileOp(),
	}
}

// Download adds the download state to the accepted states.
func (a *TorrentStoreStateAcceptor) Download() *TorrentStoreStateAcceptor {
	a.op = a.op.AcceptState(a.store.downloadState)
	return a
}

// Cache adds the cache state to the accepted states.
func (a *TorrentStoreStateAcceptor) Cache() *TorrentStoreStateAcceptor {
	a.op = a.op.AcceptState(a.store.cacheState)
	return a
}

// GetFileReader returns a reader for name.
func (a *TorrentStoreStateAcceptor) GetFileReader(name string) (FileReader, error) {
	return a.op.GetFileReader(name)
}

// GetFileStat returns file info for name.
func (a *TorrentStoreStateAcceptor) GetFileStat(name string) (os.FileInfo, error) {
	return a.op.GetFileStat(name)
}

// DeleteFile deletes name.
func (a *TorrentStoreStateAcceptor) DeleteFile(name string) error {
	return a.op.DeleteFile(name)
}

// GetMetadata returns the metadata content of md for name.
func (a *TorrentStoreStateAcceptor) GetMetadata(name string, md metadata.Metadata) error {
	return a.op.GetFileMetadata(name, md)
}

// SetMetadata writes b to metadata content of md for name.
func (a *TorrentStoreStateAcceptor) SetMetadata(
	name string, md metadata.Metadata) (updated bool, err error) {

	return a.op.SetFileMetadata(name, md)
}

// SetMetadataAt writes b to metadata content of md starting at index i for name.
func (a *TorrentStoreStateAcceptor) SetMetadataAt(
	name string, md metadata.Metadata, b []byte, offset int64) (updated bool, err error) {

	return a.op.SetFileMetadataAt(name, md, b, offset)
}

// GetOrSetMetadata returns the metadata content of md for name, or
// initializes the metadata content to b if not set.
func (a *TorrentStoreStateAcceptor) GetOrSetMetadata(name string, md metadata.Metadata) error {
	return a.op.GetOrSetFileMetadata(name, md)
}
