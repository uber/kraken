package store

import (
	"fmt"
	"io"
	"os"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store/base"
	"code.uber.internal/infra/kraken/lib/store/metadata"

	"github.com/andres-erbsen/clock"
	"github.com/docker/distribution/uuid"
	"github.com/uber-go/tally"
)

// FileReadWriter aliases base.FileReadWriter
type FileReadWriter = base.FileReadWriter

// FileReader aliases base.FileReader
type FileReader = base.FileReader

// FileStore provides an interface for LocalFileStore. Useful for mocks.
type FileStore interface {
	Config() Config

	Close()

	CreateUploadFile(fileName string, len int64) error
	GetUploadFileStat(fileName string) (os.FileInfo, error)
	GetUploadFileReader(fileName string) (FileReader, error)
	GetUploadFileReadWriter(fileName string) (FileReadWriter, error)
	MoveUploadFileToCache(fileName, targetFileName string) error
	GetUploadFileMetadata(fileName string, md metadata.Metadata) error
	SetUploadFileMetadata(fileName string, md metadata.Metadata) error
	RangeUploadMetadata(fileName string, f func(metadata.Metadata) error) error

	CreateDownloadFile(fileName string, len int64) error
	GetDownloadFileReadWriter(fileName string) (FileReadWriter, error)
	MoveDownloadFileToCache(fileName string) error

	EnsureDownloadOrCacheFilePresent(fileName string, defaultLength int64) error
	GetDownloadOrCacheFileReader(fileName string) (FileReader, error)
	DeleteDownloadOrCacheFile(fileName string) error

	CreateCacheFile(fileName string, reader io.Reader) error
	GetCacheFileReader(fileName string) (FileReader, error)
	GetCacheFilePath(fileName string) (string, error)
	GetCacheFileStat(fileName string) (os.FileInfo, error)

	States() *StateAcceptor

	InCacheError(error) bool
	InDownloadError(error) bool
}

// LocalFileStore manages all peer agent files on local disk.
type LocalFileStore struct {
	config  Config
	stats   tally.Scope
	cleanup *cleanupManager

	uploadBackend        base.FileStore
	downloadCacheBackend base.FileStore

	stateDownload agentFileState
	stateUpload   agentFileState
	stateCache    agentFileState
}

// NewLocalFileStore initializes and returns a new LocalFileStore object.
func NewLocalFileStore(config Config, stats tally.Scope) (*LocalFileStore, error) {
	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "store",
	})

	// Wipe upload directory on restart.
	os.RemoveAll(config.UploadDir)

	for _, dir := range []string{config.UploadDir, config.DownloadDir, config.CacheDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %s", dir, err)
		}
	}

	clk := clock.New()

	uploadBackend, err := base.NewLocalFileStore(clk)
	if err != nil {
		return nil, err
	}

	var downloadCacheBackend base.FileStore
	if config.LRUConfig.Enable {
		downloadCacheBackend, err = base.NewLRUFileStore(config.LRUConfig.Size, clk)
	} else {
		downloadCacheBackend, err = base.NewCASFileStore(clk)
	}
	if err != nil {
		return nil, err
	}

	stateUpload := agentFileState{directory: config.UploadDir}
	stateDownload := agentFileState{directory: config.DownloadDir}
	stateCache := agentFileState{directory: config.CacheDir}

	cleanup := newCleanupManager(clk)
	cleanup.addJob(config.DownloadCleanup, downloadCacheBackend.NewFileOp().AcceptState(stateDownload))
	cleanup.addJob(config.CacheCleanup, downloadCacheBackend.NewFileOp().AcceptState(stateCache))

	return &LocalFileStore{
		config:               config,
		stats:                stats,
		cleanup:              cleanup,
		uploadBackend:        uploadBackend,
		downloadCacheBackend: downloadCacheBackend,
		stateUpload:          stateUpload,
		stateDownload:        stateDownload,
		stateCache:           stateCache,
	}, nil
}

// Config returns configuration of the store
func (store *LocalFileStore) Config() Config {
	return store.config
}

// Close terminates goroutines started by store.
func (store *LocalFileStore) Close() {
	store.cleanup.stop()
}

// CreateUploadFile creates an empty file in upload directory with specified size.
// If file exists and is in one of the acceptable states, returns os.ErrExist.
// If file exists but not in one of the acceptable states, returns FileStateError.
func (store *LocalFileStore) CreateUploadFile(fileName string, len int64) error {
	return store.uploadBackend.NewFileOp().CreateFile(
		fileName,
		store.stateUpload,
		len)
}

// CreateDownloadFile creates an empty file in download directory with specified size.
// If file exists and is in one of the acceptable states, returns os.ErrExist.
// If file exists but not in one of the acceptable states, returns FileStateError.
func (store *LocalFileStore) CreateDownloadFile(fileName string, len int64) error {
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateDownload).CreateFile(
		fileName,
		store.stateDownload,
		len)
}

// CreateCacheFile creates a cache file given name and reader
func (store *LocalFileStore) CreateCacheFile(fileName string, r io.Reader) error {
	tmp := fmt.Sprintf("%s.%s", fileName, uuid.Generate().String())
	if err := store.CreateUploadFile(tmp, 0); err != nil {
		return err
	}
	w, err := store.GetUploadFileReadWriter(tmp)
	if err != nil {
		return err
	}
	defer w.Close()

	digester := core.NewDigester()
	r = digester.Tee(r)

	// TODO: Delete tmp file on error
	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("copy: %s", err)
	}

	actual := digester.Digest()
	expected, err := core.NewSHA256DigestFromHex(fileName)
	if err != nil {
		return fmt.Errorf("new digest from file name: %s", err)
	}
	if actual != expected {
		return fmt.Errorf("failed to verify data: digests do not match")
	}

	if err := store.MoveUploadFileToCache(tmp, fileName); err != nil {
		if !os.IsExist(err) {
			return err
		}
		// Ignore if another thread is pulling the same blob because it is normal
	}
	return nil
}

// RangeUploadMetadata ranges upload metadata.
func (store *LocalFileStore) RangeUploadMetadata(fileName string, f func(metadata.Metadata) error) error {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).RangeFileMetadata(fileName, f)
}

// GetUploadFileReader returns a FileReader for a file in upload directory.
func (store *LocalFileStore) GetUploadFileReader(fileName string) (FileReader, error) {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFileReader(fileName)
}

// GetCacheFileReader returns a FileReader for a file in cache directory.
func (store *LocalFileStore) GetCacheFileReader(fileName string) (FileReader, error) {
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).GetFileReader(fileName)
}

// GetUploadFileReadWriter returns a FileReadWriter for a file in upload directory.
func (store *LocalFileStore) GetUploadFileReadWriter(fileName string) (FileReadWriter, error) {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFileReadWriter(fileName)
}

// GetDownloadFileReadWriter returns a FileReadWriter for a file in download directory.
func (store *LocalFileStore) GetDownloadFileReadWriter(fileName string) (FileReadWriter, error) {
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateDownload).GetFileReadWriter(fileName)
}

// GetDownloadOrCacheFileReader returns a FileReader for a file in download or cache directory.
func (store *LocalFileStore) GetDownloadOrCacheFileReader(fileName string) (FileReader, error) {
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).AcceptState(store.stateDownload).GetFileReader(fileName)
}

// GetUploadFileStat returns a FileInfo of a file in upload directory.
func (store *LocalFileStore) GetUploadFileStat(fileName string) (os.FileInfo, error) {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFileStat(fileName)
}

// GetCacheFilePath returns full path of a file in cache directory.
func (store *LocalFileStore) GetCacheFilePath(fileName string) (string, error) {
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).GetFilePath(fileName)
}

// GetCacheFileStat returns a FileInfo of a file in cache directory.
func (store *LocalFileStore) GetCacheFileStat(fileName string) (os.FileInfo, error) {
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).GetFileStat(fileName)
}

// MoveUploadFileToCache moves a file from upload directory to cache directory.
func (store *LocalFileStore) MoveUploadFileToCache(fileName, targetFileName string) error {
	uploadFilePath, err := store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFilePath(fileName)
	if err != nil {
		return err
	}
	// There is a gap between file being moved to downloadCacheBackend and the in memory object still exists in
	// uploadBackend. This is fine because file names in uploadBackend are all unique.
	defer store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).DeleteFile(fileName)
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).MoveFileFrom(
		targetFileName,
		store.stateCache,
		uploadFilePath)
}

// MoveDownloadFileToCache moves a file from download directory to cache directory.
func (store *LocalFileStore) MoveDownloadFileToCache(fileName string) error {
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateDownload).MoveFile(
		fileName,
		store.stateCache)
}

// DeleteDownloadOrCacheFile deletes a download/cache file.
func (store *LocalFileStore) DeleteDownloadOrCacheFile(fileName string) error {
	op := store.downloadCacheBackend.NewFileOp().
		AcceptState(store.stateDownload).
		AcceptState(store.stateCache)
	return op.DeleteFile(fileName)
}

// EnsureDownloadOrCacheFilePresent ensures that fileName is present in either
// the download or cache state. If it is not, then it is initialized in download
// with defaultLength.
func (store *LocalFileStore) EnsureDownloadOrCacheFilePresent(fileName string, defaultLength int64) error {
	err := store.downloadCacheBackend.NewFileOp().AcceptState(store.stateDownload).AcceptState(store.stateCache).CreateFile(
		fileName,
		store.stateDownload,
		defaultLength)
	if err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

// GetUploadFileMetadata gets upload metadata.
func (store *LocalFileStore) GetUploadFileMetadata(fileName string, md metadata.Metadata) error {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFileMetadata(fileName, md)
}

// SetUploadFileMetadata sets upload metadata.
func (store *LocalFileStore) SetUploadFileMetadata(fileName string, md metadata.Metadata) error {
	_, err := store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).SetFileMetadata(fileName, md)
	return err
}

// StateAcceptor is a builder which allows LocalFileStore clients to specify which
// states an operation may be accepted within. Should only be used for read / write
// operations which are acceptable in any state.
type StateAcceptor struct {
	store *LocalFileStore
	op    base.FileOp
}

// States returns a new StateAcceptor builder.
func (store *LocalFileStore) States() *StateAcceptor {
	return &StateAcceptor{
		store: store,
		op:    store.downloadCacheBackend.NewFileOp(),
	}
}

// Download adds the download state to the accepted states.
func (a *StateAcceptor) Download() *StateAcceptor {
	a.op = a.op.AcceptState(a.store.stateDownload)
	return a
}

// Cache adds the cache state to the accepted states.
func (a *StateAcceptor) Cache() *StateAcceptor {
	a.op = a.op.AcceptState(a.store.stateCache)
	return a
}

// GetMetadata returns the metadata content of md for filename.
func (a *StateAcceptor) GetMetadata(filename string, md metadata.Metadata) error {
	return a.op.GetFileMetadata(filename, md)
}

// SetMetadata writes b to metadata content of md for filename.
func (a *StateAcceptor) SetMetadata(
	filename string, md metadata.Metadata) (updated bool, err error) {

	return a.op.SetFileMetadata(filename, md)
}

// SetMetadataAt writes b to metadata content of md starting at index i for filename.
func (a *StateAcceptor) SetMetadataAt(
	filename string, md metadata.Metadata, b []byte, offset int64) (updated bool, err error) {

	return a.op.SetFileMetadataAt(filename, md, b, offset)
}

// GetOrSetMetadata returns the metadata content of md for filename, or
// initializes the metadata content to b if not set.
func (a *StateAcceptor) GetOrSetMetadata(filename string, md metadata.Metadata) error {
	return a.op.GetOrSetFileMetadata(filename, md)
}

// InCacheError returns true for errors originating from file store operations
// which do not accept files in cache state.
func (store *LocalFileStore) InCacheError(err error) bool {
	fse, ok := err.(*base.FileStateError)
	return ok && fse.State == store.stateCache
}

// InDownloadError returns true for errors originating from file store operations
// which do not accept files in download state.
func (store *LocalFileStore) InDownloadError(err error) bool {
	fse, ok := err.(*base.FileStateError)
	return ok && fse.State == store.stateDownload
}

// GetCacheFileMetadata returns the metadata content of md for filename.
func (store *LocalFileStore) GetCacheFileMetadata(
	filename string, md metadata.Metadata) error {

	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).GetFileMetadata(filename, md)
}

// SetCacheFileMetadata writes b to metadata content of md for filename.
func (store *LocalFileStore) SetCacheFileMetadata(
	filename string, md metadata.Metadata) (updated bool, err error) {

	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).SetFileMetadata(filename, md)
}

// SetCacheFileMetadataAt writes b to metadata content of md starting at offset for filename.
func (store *LocalFileStore) SetCacheFileMetadataAt(
	filename string, md metadata.Metadata, b []byte, offset int64) (updated bool, err error) {

	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).SetFileMetadataAt(
		filename, md, b, offset)
}

// GetOrSetCacheFileMetadata returns the metadata content of md for filename, or initializes the metadata
// content to b if not set.
func (store *LocalFileStore) GetOrSetCacheFileMetadata(filename string, md metadata.Metadata) error {
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).GetOrSetFileMetadata(filename, md)
}
