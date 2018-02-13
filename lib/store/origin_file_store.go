package store

import (
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/lib/store/base"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/andres-erbsen/clock"
	"github.com/docker/distribution/uuid"
	"github.com/robfig/cron"
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

	GetCacheFileMetadata(filename string, mt MetadataType) ([]byte, error)
	SetCacheFileMetadata(filename string, mt MetadataType, b []byte) (bool, error)
	SetCacheFileMetadataAt(filename string, mt MetadataType, b []byte, offset int64) (bool, error)
	GetOrSetCacheFileMetadata(filename string, mt MetadataType, b []byte) ([]byte, error)

	// Registry specific.
	SetUploadFileStartedAt(fileName string, content []byte) error
	GetUploadFileStartedAt(fileName string) ([]byte, error)
	SetUploadFileHashState(fileName string, content []byte, algorithm string, offset string) error
	GetUploadFileHashState(fileName string, algorithm string, offset string) ([]byte, error)
	ListUploadFileHashStatePaths(fileName string) ([]string, error)

	// TODO: Functions probably no longer needed.
	ListCacheFilesByShardID(shardID string) ([]string, error)
	ListPopulatedShardIDs() ([]string, error)
}

// OriginLocalFileStore manages all origin files on local disk.
type OriginLocalFileStore struct {
	uploadBackend base.FileStore
	cacheBackend  base.FileStore
	config        OriginConfig
	clk           clock.Clock

	stateUpload agentFileState
	stateCache  agentFileState

	cacheCleanupCron *cron.Cron
}

// NewOriginFileStore initializes and returns a new OriginLocalFileStore object.
func NewOriginFileStore(config OriginConfig, clk clock.Clock) (*OriginLocalFileStore, error) {
	config = config.applyDefaults()

	err := initOriginStoreDirectories(config)
	if err != nil {
		return nil, fmt.Errorf("init origin directories: %s", err)
	}

	uploadBackend, err := base.NewLocalFileStore()
	if err != nil {
		return nil, fmt.Errorf("init origin upload backend: %s", err)
	}

	cacheBackend, err := base.NewCASFileStoreWithLRUMap(config.Capacity, clk)
	if err != nil {
		return nil, fmt.Errorf("init origin cache backend: %s", err)
	}

	originStore := &OriginLocalFileStore{
		uploadBackend: uploadBackend,
		cacheBackend:  cacheBackend,
		config:        config,
		clk:           clk,

		stateUpload: agentFileState{directory: config.UploadDir},
		stateCache:  agentFileState{directory: config.CacheDir},
	}

	// Start a cron to delete files that reached TTI.
	if config.TTI > 0 && config.CleanupInterval > 0 {
		originStore.cacheCleanupCron = cron.New()
		intervalSecs := int(math.Ceil(config.CleanupInterval.Seconds()))
		spec := fmt.Sprintf("@every %ds", intervalSecs)
		err = originStore.cacheCleanupCron.AddFunc(spec, func() {
			log.Info("Starting cache cleanup cron")
			if err := originStore.cleanupExpiredCacheFile(); err != nil {
				log.Errorf("Failed to execute cache cleanup cron: %s", err)
			}

			log.Info("Finished cache cleanup cron")
		})
		if err != nil {
			return nil, fmt.Errorf("origin cache cleanup cron: %s", err)
		}
		log.Info("Starting cache cleanup cron")
		originStore.cacheCleanupCron.Start()
	}

	return originStore, nil
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

func (store *OriginLocalFileStore) cleanupExpiredCacheFile() error {
	// Walk through the cache directory and remove expired files.
	dataDepth := base.DefaultShardIDLength + 1
	err := walkDirectory(store.config.CacheDir, dataDepth, func(currPath string) error {
		latSuffix := base.NewLastAccessTime().GetSuffix()
		latPath := path.Join(currPath, latSuffix)
		if _, err := os.Stat(latPath); os.IsNotExist(err) {
			return nil
		} else if err != nil {
			return err
		}

		b, err := ioutil.ReadFile(latPath)
		if err != nil {
			return err
		}
		lat, err := base.UnmarshalLastAccessTime(b)
		if err != nil {
			return fmt.Errorf("unmarshal lat %s: %s", b, err)
		}
		if store.clk.Now().Sub(lat) > store.config.TTI {
			fileName := filepath.Base(currPath)
			err := store.cacheBackend.NewFileOp().AcceptState(store.stateCache).DeleteFile(fileName)
			if err != nil {
				log.Errorf("Failed to clean up file %s: %s", fileName, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk cache: %s", err)
	}
	return nil
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
func (store *OriginLocalFileStore) CreateCacheFile(fileName string, reader io.Reader) error {
	tmpFile := fmt.Sprintf("%s.%s", fileName, uuid.Generate().String())
	if err := store.CreateUploadFile(tmpFile, 0); err != nil {
		return err
	}
	w, err := store.GetUploadFileReadWriter(tmpFile)
	if err != nil {
		return err
	}
	defer w.Close()

	// Stream to file and verify content at the same time
	r := io.TeeReader(reader, w)

	verified, err := image.Verify(image.NewSHA256DigestFromHex(fileName), r)
	if err != nil {
		return fmt.Errorf("origin verify image: %s", err)
	}
	if !verified {
		// TODO: Delete tmp file on error
		return fmt.Errorf("origin image digests do not match")
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

// GetCacheFileMetadata returns the metadata content of mt for filename.
func (store *OriginLocalFileStore) GetCacheFileMetadata(
	filename string, mt MetadataType) ([]byte, error) {

	return store.cacheBackend.NewFileOp().AcceptState(store.stateCache).GetFileMetadata(filename, mt)
}

// SetCacheFileMetadata writes b to metadata content of mt for filename.
func (store *OriginLocalFileStore) SetCacheFileMetadata(
	filename string, mt MetadataType, b []byte) (updated bool, err error) {

	return store.cacheBackend.NewFileOp().AcceptState(store.stateCache).SetFileMetadata(filename, mt, b)
}

// SetCacheFileMetadataAt writes b to metadata content of mt starting at offset for filename.
func (store *OriginLocalFileStore) SetCacheFileMetadataAt(
	filename string, mt MetadataType, b []byte, offset int64) (updated bool, err error) {

	n, err := store.cacheBackend.NewFileOp().AcceptState(store.stateCache).SetFileMetadataAt(filename, mt, b, offset)
	return n != 0, err
}

// GetOrSetCacheFileMetadata returns the metadata content of mt for filename, or initializes the metadata
// content to b if not set.
func (store *OriginLocalFileStore) GetOrSetCacheFileMetadata(
	filename string, mt MetadataType, b []byte) ([]byte, error) {

	return store.cacheBackend.NewFileOp().AcceptState(store.stateCache).GetOrSetFileMetadata(filename, mt, b)
}

// SetUploadFileStartedAt creates and writes creation file for a new upload file.
func (store *OriginLocalFileStore) SetUploadFileStartedAt(fileName string, content []byte) error {
	_, err := store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).SetFileMetadata(
		fileName,
		NewStartedAt(),
		content)
	return err
}

// GetUploadFileStartedAt reads creation file for a new upload file.
func (store *OriginLocalFileStore) GetUploadFileStartedAt(fileName string) ([]byte, error) {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFileMetadata(
		fileName,
		NewStartedAt())
}

// SetUploadFileHashState creates and writes hashstate for a upload file.
func (store *OriginLocalFileStore) SetUploadFileHashState(
	fileName string, content []byte, algorithm string, offset string) error {
	_, err := store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).SetFileMetadata(
		fileName,
		NewHashState(algorithm, offset),
		content)
	return err
}

// GetUploadFileHashState reads hashstate for a upload file.
func (store *OriginLocalFileStore) GetUploadFileHashState(
	fileName string, algorithm string, offset string) ([]byte, error) {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFileMetadata(
		fileName,
		NewHashState(algorithm, offset))
}

// ListUploadFileHashStatePaths list paths of all hashstates for a upload file.
// This function is not thread-safe.
// TODO: Right now we store metadata with _hashstate, but registry expects /hashstate.
func (store *OriginLocalFileStore) ListUploadFileHashStatePaths(fileName string) ([]string, error) {
	var paths []string
	store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).RangeFileMetadata(
		fileName, func(mt base.MetadataType) error {
			if re := regexp.MustCompile("_hashstates/\\w+/\\w+$"); re.MatchString(mt.GetSuffix()) {
				r := strings.NewReplacer("_", "/")
				p := path.Join("localstore/_uploads/", fileName)
				paths = append(paths, p+r.Replace(mt.GetSuffix()))
			}
			return nil
		})

	return paths, nil
}

// DeleteCacheFile deletes a file from cache directory
func (store *OriginLocalFileStore) DeleteCacheFile(fileName string) error {
	return store.cacheBackend.NewFileOp().AcceptState(store.stateCache).DeleteFile(fileName)
}

// ListCacheFilesByShardID returns a list of FileInfo for all files of given shard.
func (store *OriginLocalFileStore) ListCacheFilesByShardID(shardID string) ([]string, error) {
	shardDir := store.config.CacheDir
	for i := 0; i < len(shardID); i += 2 {
		// LocalFileStore uses the first few bytes of file digest (which is also supposed to be the file
		// name) as shard ID.
		// For every byte, one more level of directories will be created
		// (1 byte = 2 char of file name assumming file name is in HEX)
		shardDir = path.Join(shardDir, shardID[i:i+2])
	}
	infos, err := ioutil.ReadDir(shardDir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, info := range infos {
		names = append(names, info.Name())
	}
	return names, nil
}

// ListPopulatedShardIDs is a best effort function which returns the shard ids
// of all populated shards.
//
// XXX: This is an expensive operation and will potentially return stale data.
// Caller should not assume shard ids will remain populated.
func (store *OriginLocalFileStore) ListPopulatedShardIDs() ([]string, error) {
	var shards []string

	err := walkDirectory(
		store.config.CacheDir, base.DefaultShardIDLength, func(currPath string) error {
			shard := strings.TrimPrefix(currPath, store.config.CacheDir)
			shards = append(shards, strings.Replace(shard, "/", "", -1))
			return nil
		})

	return shards, err
}
