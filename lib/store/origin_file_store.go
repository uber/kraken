package store

import (
	"fmt"
	"hash"
	"io"
	"os"
	"path"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/lib/store/base"
	"code.uber.internal/infra/kraken/lib/store/metadata"

	"github.com/andres-erbsen/clock"
	"github.com/docker/distribution/uuid"
	"github.com/spaolacci/murmur3"
)

// OriginFileStore provides an interface for OriginLocalFileStore. Useful for mocks.
type OriginFileStore interface {
	Config() OriginConfig

	CreateUploadFile(fileName string, len int64) error
	GetUploadFileReader(fileName string) (FileReader, error)
	GetUploadFileReadWriter(fileName string) (FileReadWriter, error)
	GetUploadFileStat(fileName string) (os.FileInfo, error)

	CreateCacheFile(fileName string, reader io.Reader) error
	GetCacheFileReader(fileName string) (FileReader, error)
	GetCacheFileStat(fileName string) (os.FileInfo, error)
	MoveUploadFileToCache(fileName, targetFileName string) error
	DeleteCacheFile(fileName string) error

	GetCacheFileMetadata(filename string, md metadata.Metadata) error
	SetCacheFileMetadata(filename string, md metadata.Metadata) (bool, error)
	GetOrSetCacheFileMetadata(filename string, md metadata.Metadata) error

	RangeUploadMetadata(filename string, f func(metadata.Metadata) error) error
}

// OriginLocalFileStore manages all origin files on local disk.
type OriginLocalFileStore struct {
	config  OriginConfig
	clk     clock.Clock
	cleanup *cleanupManager

	uploadBackend base.FileStore
	cacheBackend  base.FileStore

	stateUpload agentFileState
	stateCache  agentFileState
}

// NewOriginFileStore initializes and returns a new OriginLocalFileStore object.
func NewOriginFileStore(config OriginConfig, clk clock.Clock) (*OriginLocalFileStore, error) {
	config = config.applyDefaults()

	if err := initOriginStoreDirectories(config); err != nil {
		return nil, fmt.Errorf("init origin directories: %s", err)
	}

	uploadBackend, err := base.NewLocalFileStore(clk)
	if err != nil {
		return nil, fmt.Errorf("init origin upload backend: %s", err)
	}

	cacheBackend, err := base.NewCASFileStoreWithLRUMap(config.Capacity, clk)
	if err != nil {
		return nil, fmt.Errorf("init origin cache backend: %s", err)
	}

	stateUpload := agentFileState{config.UploadDir}
	stateCache := agentFileState{config.CacheDir}

	cleanup := newCleanupManager(clk)
	cleanup.addJob(config.UploadCleanup, uploadBackend.NewFileOp().AcceptState(stateUpload))
	cleanup.addJob(config.CacheCleanup, cacheBackend.NewFileOp().AcceptState(stateCache))

	return &OriginLocalFileStore{
		config:        config,
		clk:           clk,
		cleanup:       cleanup,
		uploadBackend: uploadBackend,
		cacheBackend:  cacheBackend,
		stateUpload:   stateUpload,
		stateCache:    stateCache,
	}, nil
}

func initOriginStoreDirectories(config OriginConfig) error {
	// Wipe upload directory on restart.
	os.RemoveAll(config.UploadDir)

	for _, dir := range []string{config.UploadDir, config.CacheDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("mkdir %s: %s", dir, err)
		}
	}

	// If a list of volumes is provided, the volumes will be used to store the actual files, and
	// symlinks will be created from these volumes to the state directories.
	// Download, cache and trash dirs are supposed to contain 256 symlinks (first level of shards),
	// points to different volumnes based on rendezvous hash.
	if len(config.Volumes) > 0 {
		rendezvousHash := hrw.NewRendezvousHash(
			func() hash.Hash { return murmur3.New64() },
			hrw.UInt64ToFloat64)

		for _, volume := range config.Volumes {
			if _, err := os.Stat(volume.Location); err != nil {
				return fmt.Errorf("verify volume: %s", err)
			}
			rendezvousHash.AddNode(volume.Location, volume.Weight)
		}

		// Create 256 symlinks under cache dir.
		for subdirIndex := 0; subdirIndex < 256; subdirIndex++ {
			subdirName := fmt.Sprintf("%02X", subdirIndex)
			nodes, err := rendezvousHash.GetOrderedNodes(subdirName, 1)
			if len(nodes) != 1 || err != nil {
				return fmt.Errorf("calculate volume for subdir: %s", subdirName)
			}
			sourcePath := path.Join(nodes[0].Label, path.Base(config.CacheDir), subdirName)
			if err := os.MkdirAll(sourcePath, 0755); err != nil {
				return fmt.Errorf("volume source path: %s", err)
			}
			targetPath := path.Join(config.CacheDir, subdirName)
			if err := createOrUpdateSymlink(sourcePath, targetPath); err != nil {
				return fmt.Errorf("symlink to volume: %s", err)
			}
		}
	}

	return nil
}

// Close terminates goroutines started by store.
func (store *OriginLocalFileStore) Close() {
	store.cleanup.stop()
}

// Config returns configuration of the store
func (store *OriginLocalFileStore) Config() OriginConfig {
	return store.config
}

// CreateUploadFile creates an empty file in upload directory with specified size.
// If file exists and is in one of the acceptable states, returns os.ErrExist.
// If file exists but not in one of the acceptable states, returns FileStateError.
func (store *OriginLocalFileStore) CreateUploadFile(fileName string, len int64) error {
	return store.uploadBackend.NewFileOp().CreateFile(
		fileName,
		store.stateUpload,
		len)
}

// CreateCacheFile creates a cache file given name and reader
func (store *OriginLocalFileStore) CreateCacheFile(fileName string, r io.Reader) error {
	tmpFile := fmt.Sprintf("%s.%s", fileName, uuid.Generate().String())
	if err := store.CreateUploadFile(tmpFile, 0); err != nil {
		return err
	}
	w, err := store.GetUploadFileReadWriter(tmpFile)
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

	if err := store.MoveUploadFileToCache(tmpFile, fileName); err != nil {
		if !os.IsExist(err) {
			return err
		}
		// Ignore if another thread is pulling the same blob because it is normal
	}
	return nil
}

// GetUploadFileReader returns a FileReader for a file in upload directory.
func (store *OriginLocalFileStore) GetUploadFileReader(fileName string) (FileReader, error) {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFileReader(fileName)
}

// GetCacheFileReader returns a FileReader for a file in cache directory.
func (store *OriginLocalFileStore) GetCacheFileReader(fileName string) (FileReader, error) {
	return store.cacheBackend.NewFileOp().AcceptState(store.stateCache).GetFileReader(fileName)
}

// GetUploadFileReadWriter returns a FileReadWriter for a file in upload directory.
func (store *OriginLocalFileStore) GetUploadFileReadWriter(fileName string) (FileReadWriter, error) {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFileReadWriter(fileName)
}

// GetUploadFileStat returns a FileInfo of a file in upload directory.
func (store *OriginLocalFileStore) GetUploadFileStat(fileName string) (os.FileInfo, error) {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFileStat(fileName)
}

// GetCacheFileStat returns a FileInfo of a file in cache directory.
func (store *OriginLocalFileStore) GetCacheFileStat(fileName string) (os.FileInfo, error) {
	return store.cacheBackend.NewFileOp().AcceptState(store.stateCache).GetFileStat(fileName)
}

// MoveUploadFileToCache moves a file from upload directory to cache directory.
func (store *OriginLocalFileStore) MoveUploadFileToCache(fileName, targetFileName string) error {
	uploadFilePath, err := store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFilePath(fileName)
	if err != nil {
		return err
	}
	// There is a gap between file being moved to cacheBackend and the in memory object still exists
	// in uploadBackend. This is fine because file names in uploadBackend are all unique.
	defer store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).DeleteFile(fileName)
	return store.cacheBackend.NewFileOp().AcceptState(store.stateCache).MoveFileFrom(
		targetFileName,
		store.stateCache,
		uploadFilePath)
}

// GetCacheFileMetadata returns the metadata content of md for filename.
func (store *OriginLocalFileStore) GetCacheFileMetadata(filename string, md metadata.Metadata) error {
	return store.cacheBackend.NewFileOp().AcceptState(store.stateCache).GetFileMetadata(filename, md)
}

// SetCacheFileMetadata writes b to metadata content of md for filename.
func (store *OriginLocalFileStore) SetCacheFileMetadata(
	filename string, md metadata.Metadata) (updated bool, err error) {

	return store.cacheBackend.NewFileOp().AcceptState(store.stateCache).SetFileMetadata(filename, md)
}

// GetOrSetCacheFileMetadata returns the metadata content of md for filename, or initializes the metadata
// content to b if not set.
func (store *OriginLocalFileStore) GetOrSetCacheFileMetadata(filename string, md metadata.Metadata) error {
	return store.cacheBackend.NewFileOp().AcceptState(store.stateCache).GetOrSetFileMetadata(filename, md)
}

// RangeUploadMetadata ranges upload metadata.
func (store *OriginLocalFileStore) RangeUploadMetadata(
	fileName string, f func(metadata.Metadata) error) error {

	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).RangeFileMetadata(fileName, f)
}

// DeleteCacheFile deletes a file from cache directory
func (store *OriginLocalFileStore) DeleteCacheFile(fileName string) error {
	return store.cacheBackend.NewFileOp().AcceptState(store.stateCache).DeleteFile(fileName)
}
