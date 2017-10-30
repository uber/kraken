package store

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"regexp"
	"strings"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/store/base"
	"code.uber.internal/infra/kraken/lib/store/refcountable"
	"code.uber.internal/infra/kraken/utils/osutil"

	"github.com/docker/distribution/uuid"
	"github.com/robfig/cron"
)

// FileReadWriter aliases base.FileReadWriter
type FileReadWriter = base.FileReadWriter

// FileReader aliases base.FileReader
type FileReader = base.FileReader

// FileStore provides an interface for LocalFileStore. Useful for mocks.
type FileStore interface {
	Stop()
	Config() Config
	CreateUploadFile(fileName string, len int64) error
	CreateDownloadFile(fileName string, len int64) error
	CreateCacheFile(fileName string, reader io.Reader) error
	WriteDownloadFilePieceStatus(fileName string, content []byte) (bool, error)
	WriteDownloadFilePieceStatusAt(fileName string, content []byte, index int) (bool, error)
	GetFilePieceStatus(fileName string, index int, numPieces int) ([]byte, error)
	SetUploadFileStartedAt(fileName string, content []byte) error
	GetUploadFileStartedAt(fileName string) ([]byte, error)
	DeleteUploadFileStartedAt(fileName string) error
	SetUploadFileHashState(fileName string, content []byte, algorithm string, offset string) error
	GetUploadFileHashState(fileName string, algorithm string, offset string) ([]byte, error)
	ListUploadFileHashStatePaths(fileName string) ([]string, error)
	GetDownloadOrCacheFileMeta(fileName string) ([]byte, error)
	SetDownloadOrCacheFileMeta(fileName string, data []byte) (bool, error)
	GetUploadFileReader(fileName string) (base.FileReader, error)
	GetCacheFileReader(fileName string) (base.FileReader, error)
	GetUploadFileReadWriter(fileName string) (base.FileReadWriter, error)
	GetDownloadFileReadWriter(fileName string) (base.FileReadWriter, error)
	GetDownloadOrCacheFileReader(fileName string) (base.FileReader, error)
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
}

// LocalFileStore manages all peer agent files on local disk.
type LocalFileStore struct {
	uploadBackend        base.FileStore
	downloadCacheBackend base.FileStore
	config               *Config

	stateDownload agentFileState
	stateUpload   agentFileState
	stateCache    agentFileState

	trashDeletionCron *cron.Cron
}

// NewLocalFileStore initializes and returns a new LocalFileStore object.
func NewLocalFileStore(config *Config, useRefcount bool) (*LocalFileStore, error) {
	// Init all directories.
	for _, dir := range []string{config.UploadDir, config.DownloadDir, config.TrashDir} {
		os.RemoveAll(dir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatal(err)
		}
	}

	// We do not want to remove all existing files in cache directory during restart.
	err := os.MkdirAll(config.CacheDir, 0755)
	if err != nil {
		log.Fatal(err)
	}

	uploadBackend, err := base.NewShardedFileStoreDefault()
	if err != nil {
		return nil, err
	}

	var downloadCacheBackend base.FileStore
	if useRefcount {
		downloadCacheBackend, err = refcountable.NewLocalRCFileStoreDefault()
	} else if config.LRUConfig.Enable {
		downloadCacheBackend, err = base.NewLocalFileStoreWithLRU(config.LRUConfig.Size)
	} else {
		downloadCacheBackend, err = base.NewShardedFileStoreDefault()
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

// Stop stops any running cron jobs.
func (store *LocalFileStore) Stop() {
	if store.trashDeletionCron != nil {
		store.trashDeletionCron.Stop()
	}
}

// Config returns configuration of the store
func (store *LocalFileStore) Config() Config {
	return *store.config
}

// CreateUploadFile creates an empty file in upload directory with specified size.
// If file exists and is in one of the acceptable states, returns os.ErrExist.
// If file exists but not in one of the acceptable states, returns FileStateError.
func (store *LocalFileStore) CreateUploadFile(fileName string, len int64) error {
	return store.uploadBackend.CreateFile(
		fileName,
		[]base.FileState{},
		store.stateUpload,
		len)
}

// CreateDownloadFile creates an empty file in download directory with specified size.
// If file exists and is in one of the acceptable states, returns os.ErrExist.
// If file exists but not in one of the acceptable states, returns FileStateError.
func (store *LocalFileStore) CreateDownloadFile(fileName string, len int64) error {
	return store.downloadCacheBackend.CreateFile(
		fileName,
		[]base.FileState{store.stateDownload},
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
	return store.downloadCacheBackend.WriteFileMetadata(
		fileName,
		[]base.FileState{store.stateDownload},
		NewPieceStatus(),
		content)
}

// WriteDownloadFilePieceStatusAt update piece status for download file at given index.
func (store *LocalFileStore) WriteDownloadFilePieceStatusAt(fileName string, content []byte, index int) (bool, error) {
	n, err := store.downloadCacheBackend.WriteFileMetadataAt(
		fileName,
		[]base.FileState{store.stateDownload},
		NewPieceStatus(),
		content,
		int64(index))
	if n == 0 {
		return false, err
	}
	return true, err
}

// GetFilePieceStatus reads piece status for a file that's in download or cache dir.
func (store *LocalFileStore) GetFilePieceStatus(fileName string, index int, numPieces int) ([]byte, error) {
	b := make([]byte, numPieces)
	_, err := store.downloadCacheBackend.ReadFileMetadataAt(
		fileName,
		[]base.FileState{store.stateDownload},
		NewPieceStatus(),
		b,
		int64(index))
	if base.IsFileStateError(err) {
		// For files that finished downloading or were pushed, piece status should be done.
		if _, e := store.downloadCacheBackend.GetFileStat(fileName, []base.FileState{store.stateCache}); e == nil {
			for i := range b {
				b[i] = PieceDone
			}
		}
		return b, nil
	}

	return b, err
}

// SetUploadFileStartedAt creates and writes creation file for a new upload file.
func (store *LocalFileStore) SetUploadFileStartedAt(fileName string, content []byte) error {
	_, err := store.uploadBackend.WriteFileMetadata(
		fileName,
		[]base.FileState{store.stateUpload},
		NewStartedAt(),
		content)
	return err
}

// GetUploadFileStartedAt reads creation file for a new upload file.
func (store *LocalFileStore) GetUploadFileStartedAt(fileName string) ([]byte, error) {
	return store.uploadBackend.ReadFileMetadata(
		fileName,
		[]base.FileState{store.stateUpload},
		NewStartedAt())
}

// DeleteUploadFileStartedAt deletes creation file for a new upload file.
func (store *LocalFileStore) DeleteUploadFileStartedAt(fileName string) error {
	return store.uploadBackend.DeleteFileMetadata(
		fileName,
		[]base.FileState{store.stateUpload},
		NewStartedAt())
}

// SetUploadFileHashState creates and writes hashstate for a upload file.
func (store *LocalFileStore) SetUploadFileHashState(fileName string, content []byte, algorithm string, offset string) error {
	_, err := store.uploadBackend.WriteFileMetadata(
		fileName,
		[]base.FileState{store.stateUpload},
		NewHashState(algorithm, offset),
		content)
	return err
}

// GetUploadFileHashState reads hashstate for a upload file.
func (store *LocalFileStore) GetUploadFileHashState(fileName string, algorithm string, offset string) ([]byte, error) {
	return store.uploadBackend.ReadFileMetadata(
		fileName,
		[]base.FileState{store.stateUpload},
		NewHashState(algorithm, offset))
}

// ListUploadFileHashStatePaths list paths of all hashstates for a upload file.
// This function is not thread-safe.
// TODO: Right now we store metadata with _hashstate, but registry expects /hashstate.
func (store *LocalFileStore) ListUploadFileHashStatePaths(fileName string) ([]string, error) {
	fp, err := store.uploadBackend.GetFilePath(fileName, []base.FileState{store.stateUpload})
	if err != nil {
		return nil, err
	}

	var paths []string
	store.uploadBackend.RangeFileMetadata(fileName, []base.FileState{store.stateUpload}, func(mt base.MetadataType) error {
		if re := regexp.MustCompile("_hashstates/\\w+/\\w+$"); re.MatchString(mt.GetSuffix()) {
			r := strings.NewReplacer("_", "/")
			p := path.Join("localstore/_uploads/", path.Base(fp))
			paths = append(paths, p+r.Replace(mt.GetSuffix()))
		}
		return nil
	})

	return paths, nil
}

// GetDownloadOrCacheFileMeta reads filemeta from a downloading or cached file
func (store *LocalFileStore) GetDownloadOrCacheFileMeta(fileName string) ([]byte, error) {
	return store.downloadCacheBackend.ReadFileMetadata(
		fileName,
		[]base.FileState{store.stateDownload, store.stateCache},
		NewTorrentMeta(),
	)
}

// SetDownloadOrCacheFileMeta reads filemeta from a downloading or cached file
func (store *LocalFileStore) SetDownloadOrCacheFileMeta(fileName string, data []byte) (bool, error) {
	return store.downloadCacheBackend.WriteFileMetadata(
		fileName,
		[]base.FileState{store.stateDownload, store.stateCache},
		NewTorrentMeta(),
		data,
	)
}

// GetUploadFileReader returns a FileReader for a file in upload directory.
func (store *LocalFileStore) GetUploadFileReader(fileName string) (base.FileReader, error) {
	return store.uploadBackend.GetFileReader(fileName, []base.FileState{store.stateUpload})
}

// GetCacheFileReader returns a FileReader for a file in cache directory.
func (store *LocalFileStore) GetCacheFileReader(fileName string) (base.FileReader, error) {
	return store.downloadCacheBackend.GetFileReader(fileName, []base.FileState{store.stateCache})
}

// GetUploadFileReadWriter returns a FileReadWriter for a file in upload directory.
func (store *LocalFileStore) GetUploadFileReadWriter(fileName string) (base.FileReadWriter, error) {
	return store.uploadBackend.GetFileReadWriter(fileName, []base.FileState{store.stateUpload})
}

// GetDownloadFileReadWriter returns a FileReadWriter for a file in download directory.
func (store *LocalFileStore) GetDownloadFileReadWriter(fileName string) (base.FileReadWriter, error) {
	return store.downloadCacheBackend.GetFileReadWriter(fileName, []base.FileState{store.stateDownload})
}

// GetDownloadOrCacheFileReader returns a FileReader for a file in download or cache directory.
func (store *LocalFileStore) GetDownloadOrCacheFileReader(fileName string) (base.FileReader, error) {
	return store.downloadCacheBackend.GetFileReader(fileName, []base.FileState{store.stateDownload, store.stateCache})
}

// GetUploadFileStat returns a FileInfo of a file in upload directory.
func (store *LocalFileStore) GetUploadFileStat(fileName string) (os.FileInfo, error) {
	return store.uploadBackend.GetFileStat(fileName, []base.FileState{store.stateUpload})
}

// GetCacheFilePath returns full path of a file in cache directory.
func (store *LocalFileStore) GetCacheFilePath(fileName string) (string, error) {
	return store.downloadCacheBackend.GetFilePath(fileName, []base.FileState{store.stateCache})
}

// GetCacheFileStat returns a FileInfo of a file in cache directory.
func (store *LocalFileStore) GetCacheFileStat(fileName string) (os.FileInfo, error) {
	return store.downloadCacheBackend.GetFileStat(fileName, []base.FileState{store.stateCache})
}

// MoveUploadFileToCache moves a file from upload directory to cache directory.
func (store *LocalFileStore) MoveUploadFileToCache(fileName, targetFileName string) error {
	uploadFilePath, err := store.uploadBackend.GetFilePath(fileName, []base.FileState{store.stateUpload})
	if err != nil {
		return err
	}
	err = store.downloadCacheBackend.CreateLinkFromFile(
		targetFileName,
		[]base.FileState{store.stateCache},
		store.stateCache,
		uploadFilePath)
	if err != nil {
		return err
	}
	err = store.uploadBackend.DeleteFile(fileName, []base.FileState{store.stateUpload})
	return err
}

// MoveDownloadFileToCache moves a file from download directory to cache directory.
func (store *LocalFileStore) MoveDownloadFileToCache(fileName string) error {
	return store.downloadCacheBackend.MoveFile(
		fileName,
		[]base.FileState{store.stateDownload},
		store.stateCache)
}

// MoveCacheFileToTrash moves a file from cache directory to trash directory, and append a random
// suffix so there won't be name collision.
func (store *LocalFileStore) MoveCacheFileToTrash(fileName string) error {
	newPath := path.Join(store.config.TrashDir, fileName+"."+uuid.Generate().String())
	if err := store.downloadCacheBackend.LinkToFile(fileName, []base.FileState{store.stateCache}, newPath); err != nil {
		return err
	}
	return store.downloadCacheBackend.DeleteFile(fileName, []base.FileState{store.stateCache})
}

// MoveDownloadOrCacheFileToTrash moves a file from cache or download directory to trash directory, and append a random
// suffix so there won't be name collision.
func (store *LocalFileStore) MoveDownloadOrCacheFileToTrash(fileName string) error {
	newPath := path.Join(store.config.TrashDir, fileName+"."+uuid.Generate().String())
	if err := store.downloadCacheBackend.LinkToFile(fileName, []base.FileState{store.stateCache, store.stateDownload}, newPath); err != nil {
		return err
	}
	return store.downloadCacheBackend.DeleteFile(fileName, []base.FileState{store.stateCache, store.stateDownload})
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
		err = os.Remove(path.Join(store.config.TrashDir, fileName))
		if err != nil {
			return err
		}
	}
	return nil
}

// RefCacheFile increments ref count for a file in cache directory.
func (store *LocalFileStore) RefCacheFile(fileName string) (int64, error) {
	b, ok := store.downloadCacheBackend.(refcountable.RCFileStore)
	if !ok {
		return 0, fmt.Errorf("Local ref count is disabled")
	}
	return b.IncrementFileRefCount(fileName, []base.FileState{store.stateCache})
}

// DerefCacheFile decrements ref count for a file in cache directory.
// If ref count reaches 0, it will try to rename it and move it to trash directory.
func (store *LocalFileStore) DerefCacheFile(fileName string) (int64, error) {
	b, ok := store.downloadCacheBackend.(refcountable.RCFileStore)
	if !ok {
		return 0, fmt.Errorf("Local ref count is disabled")
	}
	refCount, err := b.DecrementFileRefCount(fileName, []base.FileState{store.stateCache})
	if err == nil && refCount == 0 {
		// Try rename and move to trash.
		newPath := path.Join(store.config.TrashDir, fileName+"."+uuid.Generate().String())
		if err := b.LinkToFile(fileName, []base.FileState{store.stateCache}, newPath); err != nil {
			return 0, err
		}
		err := b.DeleteFile(fileName, []base.FileState{store.stateCache})
		if refcountable.IsRefCountError(err) {
			// It's possible ref count was incremented again, and that's normal. Abort.
			return err.(*refcountable.RefCountError).RefCount, nil
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
