package store

import (
	"fmt"
	"log"
	"os"
	"path"
	"regexp"
	"strings"

	"code.uber.internal/infra/kraken/client/store/base"
	"code.uber.internal/infra/kraken/client/store/refcountable"
	"code.uber.internal/infra/kraken/configuration"

	"github.com/docker/distribution/uuid"
)

// LocalStore manages all peer agent files on local disk.
type LocalStore struct {
	uploadBackend        base.FileStore
	downloadCacheBackend base.FileStore
	config               *configuration.Config

	stateDownload agentFileState
	stateUpload   agentFileState
	stateCache    agentFileState
}

// NewLocalStore initializes and returns a new LocalStore object.
func NewLocalStore(config *configuration.Config) *LocalStore {
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

	uploadBackend := base.NewLocalFileStoreDefault()
	var downloadCacheBackend base.FileStore
	if config.TagDeletion.Enable {
		downloadCacheBackend = refcountable.NewLocalRCFileStoreDefault()
	} else {
		downloadCacheBackend = base.NewLocalFileStoreDefault()
	}
	return &LocalStore{
		uploadBackend:        uploadBackend,
		downloadCacheBackend: downloadCacheBackend,
		config:               config,
		stateUpload:          agentFileState{directory: config.UploadDir},
		stateDownload:        agentFileState{directory: config.DownloadDir},
		stateCache:           agentFileState{directory: config.CacheDir},
	}
}

func (store *LocalStore) String() string {
	return fmt.Sprintf("LocalStore downloadDir %s", store.config.DownloadDir)
}

// CreateUploadFile creates an empty file in upload directory with specified size.
// If file exists and is in one of the acceptable states, returns os.ErrExist.
// If file exists but not in one of the acceptable states, returns FileStateError.
func (store *LocalStore) CreateUploadFile(fileName string, len int64) error {
	return store.uploadBackend.CreateFile(
		fileName,
		[]base.FileState{},
		store.stateUpload,
		len)
}

// CreateDownloadFile creates an empty file in download directory with specified size.
// If file exists and is in one of the acceptable states, returns os.ErrExist.
// If file exists but not in one of the acceptable states, returns FileStateError.
func (store *LocalStore) CreateDownloadFile(fileName string, len int64) error {
	return store.downloadCacheBackend.CreateFile(
		fileName,
		[]base.FileState{store.stateDownload},
		store.stateDownload,
		len)
}

// WriteDownloadFilePieceStatus creates or overwrites piece status for a new download file.
func (store *LocalStore) WriteDownloadFilePieceStatus(fileName string, content []byte) (bool, error) {
	return store.downloadCacheBackend.WriteFileMetadata(
		fileName,
		[]base.FileState{store.stateDownload},
		NewPieceStatus(),
		content)
}

// WriteDownloadFilePieceStatusAt update piece status for download file at given index.
func (store *LocalStore) WriteDownloadFilePieceStatusAt(fileName string, content []byte, index int) (bool, error) {
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
func (store *LocalStore) GetFilePieceStatus(fileName string, index int, numPieces int) ([]byte, error) {
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
func (store *LocalStore) SetUploadFileStartedAt(fileName string, content []byte) error {
	_, err := store.uploadBackend.WriteFileMetadata(
		fileName,
		[]base.FileState{store.stateUpload},
		NewStartedAt(),
		content)
	return err
}

// GetUploadFileStartedAt reads creation file for a new upload file.
func (store *LocalStore) GetUploadFileStartedAt(fileName string) ([]byte, error) {
	return store.uploadBackend.ReadFileMetadata(
		fileName,
		[]base.FileState{store.stateUpload},
		NewStartedAt())
}

// DeleteUploadFileStartedAt deletes creation file for a new upload file.
func (store *LocalStore) DeleteUploadFileStartedAt(fileName string) error {
	return store.uploadBackend.DeleteFileMetadata(
		fileName,
		[]base.FileState{store.stateUpload},
		NewStartedAt())
}

// SetUploadFileHashState creates and writes hashstate for a upload file.
func (store *LocalStore) SetUploadFileHashState(fileName string, content []byte, algorithm string, offset string) error {
	_, err := store.uploadBackend.WriteFileMetadata(
		fileName,
		[]base.FileState{store.stateUpload},
		NewHashState(algorithm, offset),
		content)
	return err
}

// GetUploadFileHashState reads hashstate for a upload file.
func (store *LocalStore) GetUploadFileHashState(fileName string, algorithm string, offset string) ([]byte, error) {
	return store.uploadBackend.ReadFileMetadata(
		fileName,
		[]base.FileState{store.stateUpload},
		NewHashState(algorithm, offset))
}

// ListUploadFileHashStatePaths list paths of all hashstates for a upload file.
// This function is not thread-safe.
// TODO: Right now we store metadata with _hashstate, but registry expects /hashstate.
func (store *LocalStore) ListUploadFileHashStatePaths(fileName string) ([]string, error) {
	fp, err := store.uploadBackend.GetFilePath(fileName, []base.FileState{store.stateUpload})
	if err != nil {
		return nil, err
	}

	var paths []string
	store.uploadBackend.RangeFileMetadata(fileName, []base.FileState{store.stateUpload}, func(mt base.MetadataType) error {
		if re := regexp.MustCompile("_hashstates/\\w+/\\w+$"); re.MatchString(mt.GetSuffix()) {
			r := strings.NewReplacer("_", "/")
			paths = append(paths, fp+r.Replace(mt.GetSuffix()))
		}
		return nil
	})

	return paths, nil
}

// GetDownloadOrCacheFileMeta reads filemeta from a downloading or cached file
func (store *LocalStore) GetDownloadOrCacheFileMeta(fileName string) ([]byte, error) {
	return store.downloadCacheBackend.ReadFileMetadata(
		fileName,
		[]base.FileState{store.stateDownload, store.stateCache},
		NewTorrentMeta(),
	)
}

// SetDownloadOrCacheFileMeta reads filemeta from a downloading or cached file
func (store *LocalStore) SetDownloadOrCacheFileMeta(fileName string, data []byte) (bool, error) {
	return store.downloadCacheBackend.WriteFileMetadata(
		fileName,
		[]base.FileState{store.stateDownload, store.stateCache},
		NewTorrentMeta(),
		data,
	)
}

// GetUploadFileReader returns a FileReader for a file in upload directory.
func (store *LocalStore) GetUploadFileReader(fileName string) (base.FileReader, error) {
	return store.uploadBackend.GetFileReader(fileName, []base.FileState{store.stateUpload})
}

// GetCacheFileReader returns a FileReader for a file in cache directory.
func (store *LocalStore) GetCacheFileReader(fileName string) (base.FileReader, error) {
	return store.downloadCacheBackend.GetFileReader(fileName, []base.FileState{store.stateCache})
}

// GetUploadFileReadWriter returns a FileReadWriter for a file in upload directory.
func (store *LocalStore) GetUploadFileReadWriter(fileName string) (base.FileReadWriter, error) {
	return store.uploadBackend.GetFileReadWriter(fileName, []base.FileState{store.stateUpload})
}

// GetDownloadFileReadWriter returns a FileReadWriter for a file in download directory.
func (store *LocalStore) GetDownloadFileReadWriter(fileName string) (base.FileReadWriter, error) {
	return store.downloadCacheBackend.GetFileReadWriter(fileName, []base.FileState{store.stateDownload})
}

// GetDownloadOrCacheFileReader returns a FileReader for a file in download or cache directory.
func (store *LocalStore) GetDownloadOrCacheFileReader(fileName string) (base.FileReader, error) {
	return store.downloadCacheBackend.GetFileReader(fileName, []base.FileState{store.stateDownload, store.stateCache})
}

// GetCacheFilePath returns full path of a file in cache directory.
func (store *LocalStore) GetCacheFilePath(fileName string) (string, error) {
	return store.downloadCacheBackend.GetFilePath(fileName, []base.FileState{store.stateCache})
}

// GetCacheFileStat returns a FileInfo of a file in cache directory.
func (store *LocalStore) GetCacheFileStat(fileName string) (os.FileInfo, error) {
	return store.downloadCacheBackend.GetFileStat(fileName, []base.FileState{store.stateCache})
}

// MoveUploadFileToCache moves a file from upload directory to cache directory.
func (store *LocalStore) MoveUploadFileToCache(fileName, targetFileName string) error {
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
func (store *LocalStore) MoveDownloadFileToCache(fileName string) error {
	return store.downloadCacheBackend.MoveFile(
		fileName,
		[]base.FileState{store.stateDownload},
		store.stateCache)
}

// MoveCacheFileToTrash moves a file from cache directory to trash directory, and append a random
// suffix so there won't be name collision.
func (store *LocalStore) MoveCacheFileToTrash(fileName string) error {
	newPath := path.Join(store.config.TrashDir, fileName+"."+uuid.Generate().String())
	if err := store.downloadCacheBackend.LinkToFile(fileName, []base.FileState{store.stateCache}, newPath); err != nil {
		return err
	}
	return store.downloadCacheBackend.DeleteFile(fileName, []base.FileState{store.stateCache})
}

// MoveDownloadOrCacheFileToTrash moves a file from cache or download directory to trash directory, and append a random
// suffix so there won't be name collision.
func (store *LocalStore) MoveDownloadOrCacheFileToTrash(fileName string) error {
	newPath := path.Join(store.config.TrashDir, fileName+"."+uuid.Generate().String())
	if err := store.downloadCacheBackend.LinkToFile(fileName, []base.FileState{store.stateCache, store.stateDownload}, newPath); err != nil {
		return err
	}
	return store.downloadCacheBackend.DeleteFile(fileName, []base.FileState{store.stateCache, store.stateDownload})
}

// DeleteAllTrashFiles permanently deletes all files from trash directory.
// This function is not executed inside global lock, and expects to be the only one doing deletion.
func (store *LocalStore) DeleteAllTrashFiles() error {
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
func (store *LocalStore) RefCacheFile(fileName string) (int64, error) {
	b, ok := store.downloadCacheBackend.(refcountable.RCFileStore)
	if !ok {
		return 0, fmt.Errorf("Local ref count is disabled")
	}
	return b.IncrementFileRefCount(fileName, []base.FileState{store.stateCache})
}

// DerefCacheFile decrements ref count for a file in cache directory.
// If ref count reaches 0, it will try to rename it and move it to trash directory.
func (store *LocalStore) DerefCacheFile(fileName string) (int64, error) {
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
