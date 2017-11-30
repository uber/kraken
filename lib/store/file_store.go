package store

import (
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"regexp"
	"strings"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/lib/store/internal"
	"code.uber.internal/infra/kraken/utils/osutil"

	"github.com/docker/distribution/uuid"
	"github.com/robfig/cron"
	"github.com/spaolacci/murmur3"
)

// FileReadWriter aliases internal.FileReadWriter
type FileReadWriter = internal.FileReadWriter

// FileReader aliases internal.FileReader
type FileReader = internal.FileReader

// MetadataType aliases internal.MetadataType
type MetadataType = internal.MetadataType

// FileReaderCloner provides access to distinct IO interfaces for a single file.
// If clients need to read a file multiple times, a single reader is not sufficient
// because it can only be read once. Loading the file into a reusable buffer
// is not an option because the size of may be in the magnitude of gigabytes.
// Thus, FileReaderCloner allows clients to retrieve as many readers as they need.
type FileReaderCloner interface {
	Clone() (FileReader, error)
}

type cacheReaderCloner struct {
	fs   FileStore
	name string
}

func (c *cacheReaderCloner) Clone() (FileReader, error) {
	return c.fs.GetCacheFileReader(c.name)
}

// CacheReaderCloner returns a FileReaderCloner for cloning FileReaders
// for name from the cache directory.
func CacheReaderCloner(fs FileStore, name string) FileReaderCloner {
	return &cacheReaderCloner{fs, name}
}

// FileStore provides an interface for LocalFileStore. Useful for mocks.
type FileStore interface {
	Config() Config
	CreateUploadFile(fileName string, len int64) error
	CreateDownloadFile(fileName string, len int64) error
	CreateCacheFile(fileName string, reader io.Reader) error
	WriteDownloadFilePieceStatus(fileName string, content []byte) (bool, error)
	WriteDownloadFilePieceStatusAt(fileName string, content []byte, index int) (bool, error)
	SetUploadFileStartedAt(fileName string, content []byte) error
	GetUploadFileStartedAt(fileName string) ([]byte, error)
	SetUploadFileHashState(fileName string, content []byte, algorithm string, offset string) error
	GetUploadFileHashState(fileName string, algorithm string, offset string) ([]byte, error)
	ListUploadFileHashStatePaths(fileName string) ([]string, error)
	GetDownloadOrCacheFileMeta(fileName string) ([]byte, error)
	SetDownloadOrCacheFileMeta(fileName string, data []byte) (bool, error)
	GetUploadFileReader(fileName string) (FileReader, error)
	GetCacheFileReader(fileName string) (FileReader, error)
	GetUploadFileReadWriter(fileName string) (FileReadWriter, error)
	GetDownloadFileReadWriter(fileName string) (FileReadWriter, error)
	GetDownloadOrCacheFileReader(fileName string) (FileReader, error)
	GetUploadFileStat(fileName string) (os.FileInfo, error)
	GetCacheFilePath(fileName string) (string, error)
	GetCacheFileStat(fileName string) (os.FileInfo, error)
	MoveUploadFileToCache(fileName, targetFileName string) error
	MoveDownloadFileToCache(fileName string) error
	MoveCacheFileToTrash(fileName string) error
	MoveDownloadOrCacheFileToTrash(fileName string) error
	DeleteAllTrashFiles() error
	RefCacheFile(fileName string) (int64, error)
	DerefCacheFile(fileName string) (int64, error)
	ListCacheFilesByShardID(shardID string) ([]string, error)
	ListPopulatedShardIDs() ([]string, error)
	EnsureDownloadOrCacheFilePresent(fileName string, defaultLength int64) error
	States() *StateAcceptor
	InCacheError(error) bool
}

// LocalFileStore manages all peer agent files on local disk.
type LocalFileStore struct {
	uploadBackend        internal.FileStore
	downloadCacheBackend internal.FileStore
	config               *Config

	stateDownload agentFileState
	stateUpload   agentFileState
	stateCache    agentFileState

	trashDeletionCron *cron.Cron
}

// NewLocalFileStore initializes and returns a new LocalFileStore object.
func NewLocalFileStore(config *Config, useRefcount bool) (*LocalFileStore, error) {
	err := initDirectories(config)
	if err != nil {
		return nil, err
	}

	uploadBackend, err := internal.NewLocalFileStore()
	if err != nil {
		return nil, err
	}

	var downloadCacheBackend internal.FileStore
	if useRefcount {
		downloadCacheBackend, err = internal.NewLocalRCFileStore()
	} else if config.LRUConfig.Enable {
		downloadCacheBackend, err = internal.NewLRUFileStore(config.LRUConfig.Size)
	} else {
		downloadCacheBackend, err = internal.NewCASFileStore()
	}
	if err != nil {
		return nil, err
	}

	localStore := &LocalFileStore{
		uploadBackend:        uploadBackend,
		downloadCacheBackend: downloadCacheBackend,
		config:               config,
		stateUpload:          agentFileState{directory: config.UploadDir},
		stateDownload:        agentFileState{directory: config.DownloadDir},
		stateCache:           agentFileState{directory: config.CacheDir},
	}

	// Start a cron to delete trash files.
	if config.TrashDeletion.Enable && config.TrashDeletion.Interval > 0 {
		localStore.trashDeletionCron = cron.New()
		intervalSecs := int(math.Ceil(config.TrashDeletion.Interval.Seconds()))
		spec := fmt.Sprintf("@every %ds", intervalSecs)
		err = localStore.trashDeletionCron.AddFunc(spec, func() {
			log.Debug("Deleting all trash files...")
			if err := localStore.DeleteAllTrashFiles(); err != nil {
				log.Errorf("Failed to delete all trash files: %s", err)
			}
		})
		if err != nil {
			return nil, err
		}
		log.Info("Starting trash cleanup cron")
		localStore.trashDeletionCron.Start()
	}

	return localStore, nil
}

func initDirectories(config *Config) error {
	// Recreate upload, download and trash dirs.
	for _, dir := range []string{config.UploadDir, config.DownloadDir, config.TrashDir} {
		os.RemoveAll(dir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	// We do not want to remove all existing files in cache directory during restart.
	err := os.MkdirAll(config.CacheDir, 0755)
	if err != nil {
		return err
	}

	// If a list of volumes is provided, the volumes will be used to store the actual files, and symlinks will be
	// created from these volumes to the state directories.
	// Download, cache and trash dirs are supposed to contain 256 symlinks (first level of shards), points to different
	// volumnes based on rendezvous hash.
	if len(config.Volumes) > 0 {
		rendezvousHash := hrw.NewRendezvousHash(
			func() hash.Hash { return murmur3.New64() },
			hrw.UInt64ToFloat64)

		for _, volume := range config.Volumes {
			if _, err := os.Stat(volume.Location); err != nil {
				return err
			}
			rendezvousHash.AddNode(volume.Location, volume.Weight)
		}

		// For download, cache and trash directories, create 256 symlinks each.
		for subdirIndex := 0; subdirIndex < 256; subdirIndex++ {
			subdirName := fmt.Sprintf("%02X", subdirIndex)
			nodes, err := rendezvousHash.GetOrderedNodes(subdirName, 1)
			if len(nodes) != 1 || err != nil {
				return fmt.Errorf("Failed to calculate volume for subdir %s", subdirName)
			}
			for _, stateDir := range []string{config.DownloadDir, config.CacheDir, config.TrashDir} {
				sourcePath := path.Join(nodes[0].Label, path.Base(stateDir), subdirName)
				if err := os.MkdirAll(sourcePath, 0755); err != nil {
					return err
				}
				targetPath := path.Join(stateDir, subdirName)
				if err := createOrUpdateSymlink(sourcePath, targetPath); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func createOrUpdateSymlink(sourcePath, targetPath string) error {
	if _, err := os.Stat(targetPath); err == nil {
		if existingSource, err := os.Readlink(targetPath); err != nil {
			return err
		} else if existingSource != sourcePath {
			// If the symlink already exists and points to another valid location, recreate the symlink.
			if err := os.Remove(targetPath); err != nil {
				return err
			}
			if err := os.Symlink(sourcePath, targetPath); err != nil {
				return err
			}
		}
	} else if os.IsNotExist(err) {
		if err := os.Symlink(sourcePath, targetPath); err != nil {
			return err
		}
	} else {
		return err
	}

	return nil
}

// Config returns configuration of the store
func (store *LocalFileStore) Config() Config {
	return *store.config
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
func (store *LocalFileStore) CreateCacheFile(fileName string, reader io.Reader) error {
	tmp := fmt.Sprintf("%s.%s", fileName, uuid.Generate().String())
	if err := store.CreateUploadFile(tmp, 0); err != nil {
		return err
	}
	w, err := store.GetUploadFileReadWriter(tmp)
	if err != nil {
		return err
	}
	defer w.Close()

	// Stream to file and verify content at the same time
	r := io.TeeReader(reader, w)

	verified, err := image.Verify(image.NewSHA256DigestFromHex(fileName), r)
	if err != nil {
		return err
	}
	if !verified {
		// TODO: Delete tmp file on error
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

// WriteDownloadFilePieceStatus creates or overwrites piece status for a new download file.
func (store *LocalFileStore) WriteDownloadFilePieceStatus(fileName string, content []byte) (bool, error) {
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateDownload).SetFileMetadata(
		fileName,
		NewPieceStatus(),
		content)
}

// WriteDownloadFilePieceStatusAt update piece status for download file at given index.
func (store *LocalFileStore) WriteDownloadFilePieceStatusAt(fileName string, content []byte, index int) (bool, error) {
	n, err := store.downloadCacheBackend.NewFileOp().AcceptState(store.stateDownload).SetFileMetadataAt(
		fileName,
		NewPieceStatus(),
		content,
		int64(index))
	if n == 0 {
		return false, err
	}
	return true, err
}

// SetUploadFileStartedAt creates and writes creation file for a new upload file.
func (store *LocalFileStore) SetUploadFileStartedAt(fileName string, content []byte) error {
	_, err := store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).SetFileMetadata(
		fileName,
		NewStartedAt(),
		content)
	return err
}

// GetUploadFileStartedAt reads creation file for a new upload file.
func (store *LocalFileStore) GetUploadFileStartedAt(fileName string) ([]byte, error) {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFileMetadata(
		fileName,
		NewStartedAt())
}

// SetUploadFileHashState creates and writes hashstate for a upload file.
func (store *LocalFileStore) SetUploadFileHashState(fileName string, content []byte, algorithm string, offset string) error {
	_, err := store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).SetFileMetadata(
		fileName,
		NewHashState(algorithm, offset),
		content)
	return err
}

// GetUploadFileHashState reads hashstate for a upload file.
func (store *LocalFileStore) GetUploadFileHashState(fileName string, algorithm string, offset string) ([]byte, error) {
	return store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).GetFileMetadata(
		fileName,
		NewHashState(algorithm, offset))
}

// ListUploadFileHashStatePaths list paths of all hashstates for a upload file.
// This function is not thread-safe.
// TODO: Right now we store metadata with _hashstate, but registry expects /hashstate.
func (store *LocalFileStore) ListUploadFileHashStatePaths(fileName string) ([]string, error) {
	var paths []string
	store.uploadBackend.NewFileOp().AcceptState(store.stateUpload).RangeFileMetadata(
		fileName, func(mt internal.MetadataType) error {
			if re := regexp.MustCompile("_hashstates/\\w+/\\w+$"); re.MatchString(mt.GetSuffix()) {
				r := strings.NewReplacer("_", "/")
				p := path.Join("localstore/_uploads/", fileName)
				paths = append(paths, p+r.Replace(mt.GetSuffix()))
			}
			return nil
		})

	return paths, nil
}

// GetDownloadOrCacheFileMeta reads filemeta from a downloading or cached file
func (store *LocalFileStore) GetDownloadOrCacheFileMeta(fileName string) ([]byte, error) {
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateDownload).AcceptState(store.stateCache).GetFileMetadata(
		fileName,
		NewTorrentMeta(),
	)
}

// SetDownloadOrCacheFileMeta reads filemeta from a downloading or cached file
func (store *LocalFileStore) SetDownloadOrCacheFileMeta(fileName string, data []byte) (bool, error) {
	return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateDownload).AcceptState(store.stateCache).SetFileMetadata(
		fileName,
		NewTorrentMeta(),
		data,
	)
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

// MoveCacheFileToTrash moves a file from cache directory to trash directory, and append a random
// suffix so there won't be name collision.
func (store *LocalFileStore) MoveCacheFileToTrash(fileName string) error {
	newPath := path.Join(store.config.TrashDir, fileName, internal.DefaultDataFileName)
	err := store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).MoveFileTo(fileName, newPath)
	if os.IsExist(err) {
		return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).DeleteFile(fileName)
	}
	return err
}

// MoveDownloadOrCacheFileToTrash moves a file from cache or download directory to trash directory, and append a random
// suffix so there won't be name collision.
func (store *LocalFileStore) MoveDownloadOrCacheFileToTrash(fileName string) error {
	newPath := path.Join(store.config.TrashDir, fileName, internal.DefaultDataFileName)
	err := store.downloadCacheBackend.NewFileOp().AcceptState(store.stateDownload).AcceptState(store.stateCache).MoveFileTo(
		fileName, newPath)
	if os.IsExist(err) {
		return store.downloadCacheBackend.NewFileOp().AcceptState(store.stateCache).DeleteFile(fileName)
	}
	return err
}

// DeleteAllTrashFiles permanently deletes all files from trash directory.
// This function is not executed inside global lock, and expects to be the only one doing deletion.
func (store *LocalFileStore) DeleteAllTrashFiles() error {
	dir, err := os.Open(store.config.TrashDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, fileName := range names {
		err = os.RemoveAll(path.Join(store.config.TrashDir, fileName))
		if err != nil {
			return err
		}
	}
	return nil
}

// RefCacheFile increments ref count for a file in cache directory.
func (store *LocalFileStore) RefCacheFile(fileName string) (int64, error) {
	op := store.downloadCacheBackend.NewFileOp()
	rcOp, ok := op.AcceptState(store.stateCache).(internal.RCFileOp)
	if !ok {
		return 0, fmt.Errorf("Local ref count is disabled")
	}
	return rcOp.IncFileRefCount(fileName)
}

// DerefCacheFile decrements ref count for a file in cache directory.
// If ref count reaches 0, it will try to rename it and move it to trash directory.
func (store *LocalFileStore) DerefCacheFile(fileName string) (int64, error) {
	op := store.downloadCacheBackend.NewFileOp()
	rcOp, ok := op.AcceptState(store.stateCache).(internal.RCFileOp)
	if !ok {
		return 0, fmt.Errorf("Local ref count is disabled")
	}
	refCount, err := rcOp.DecFileRefCount(fileName)
	if err == nil && refCount == 0 {
		// Try rename and move to trash.
		newPath := path.Join(store.config.TrashDir, fileName, internal.DefaultDataFileName)
		if err := rcOp.MoveFileTo(fileName, newPath); err != nil {
			return 0, err
		}
	}
	return refCount, nil
}

// ListCacheFilesByShardID returns a list of FileInfo for all files of given shard.
func (store *LocalFileStore) ListCacheFilesByShardID(shardID string) ([]string, error) {
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
func (store *LocalFileStore) ListPopulatedShardIDs() ([]string, error) {
	shardDir := store.config.CacheDir
	var shards []string

	// Recursive closure which walks the shard directory and adds any populated
	// shard ids to shards.
	var walk func(string, int) error
	walk = func(cursor string, depth int) error {
		dir := path.Join(shardDir, cursor)
		if depth == 0 {
			empty, err := osutil.IsEmpty(dir)
			if err != nil {
				return err
			}
			if !empty {
				shard := strings.Replace(cursor, "/", "", -1)
				shards = append(shards, shard)
			}
			return nil
		}
		infos, err := ioutil.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, info := range infos {
			if info.IsDir() {
				if err := walk(path.Join(cursor, info.Name()), depth-1); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// TODO(codyg): Revisit shard depth constant.
	err := walk("", 2)

	return shards, err
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

// StateAcceptor is a builder which allows LocalFileStore clients to specify which
// states an operation may be accepted within. Should only be used for read / write
// operations which are acceptable in any state.
type StateAcceptor struct {
	store *LocalFileStore
	op    internal.FileOp
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

// GetMetadata returns the metadata content of mt for filename.
func (a *StateAcceptor) GetMetadata(filename string, mt MetadataType) ([]byte, error) {
	return a.op.GetFileMetadata(filename, mt)
}

// SetMetadata writes b to metadata content of mt for filename.
func (a *StateAcceptor) SetMetadata(filename string, mt MetadataType, b []byte) (updated bool, err error) {
	return a.op.SetFileMetadata(filename, mt, b)
}

// SetMetadataAt writes b to metadata content of mt starting at index i for filename.
func (a *StateAcceptor) SetMetadataAt(
	filename string, mt MetadataType, b []byte, i int) (updated bool, err error) {

	n, err := a.op.SetFileMetadataAt(filename, mt, b, int64(i))
	return n != 0, err
}

// GetOrSetMetadata returns the metadata content of mt for filename, or
// initializes the metadata content to b if not set.
func (a *StateAcceptor) GetOrSetMetadata(filename string, mt MetadataType, b []byte) ([]byte, error) {
	return a.op.GetOrSetFileMetadata(filename, mt, b)
}

// InCacheError returns true for errors originating from file store operations
// which do not accept files in cache state.
func (store *LocalFileStore) InCacheError(err error) bool {
	fse, ok := err.(*internal.FileStateError)
	return ok && fse.State == store.stateCache
}
