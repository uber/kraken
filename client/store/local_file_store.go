package store

import (
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/docker/distribution/uuid"

	"code.uber.internal/infra/kraken/configuration"
)

// LocalFileStore manages all agent files on local disk.
type LocalFileStore struct {
	backend FileStoreBackend
}

// NewLocalFileStore initializes and returns a new FileStoreBackend object.
func NewLocalFileStore(config *configuration.Config) *LocalFileStore {
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

	registerFileState(stateUpload, config.UploadDir)
	registerFileState(stateDownload, config.DownloadDir)
	registerFileState(stateCache, config.CacheDir)
	registerFileState(stateTrash, config.TrashDir)

	return &LocalFileStore{
		backend: NewLocalFileStoreBackend(),
	}
}

// CreateUploadFile creates an empty file in upload directory with specified size.
func (store *LocalFileStore) CreateUploadFile(fileName string, len int64) (bool, error) {
	return store.backend.CreateFile(fileName, []FileState{}, stateUpload, len)
}

// CreateDownloadFile creates an empty file in download directory with specified size.
func (store *LocalFileStore) CreateDownloadFile(fileName string, len int64) (bool, error) {
	return store.backend.CreateFile(fileName, []FileState{stateCache}, stateDownload, len)
}

// WriteDownloadFilePieceStatus creates or overwrites piece status for a new download file.
func (store *LocalFileStore) WriteDownloadFilePieceStatus(fileName string, content []byte) (bool, error) {
	return store.backend.WriteFileMetadata(fileName, []FileState{stateDownload}, getPieceStatus(), content)
}

// WriteDownloadFilePieceStatusAt update piece status for download file at given index.
func (store *LocalFileStore) WriteDownloadFilePieceStatusAt(fileName string, content []byte, index int) (bool, error) {
	n, err := store.backend.WriteFileMetadataAt(fileName, []FileState{stateDownload}, getPieceStatus(), content, int64(index))
	if n == 0 {
		return false, err
	}
	return true, err
}

// GetFilePieceStatus reads piece status for a file that's in download or cache dir.
func (store *LocalFileStore) GetFilePieceStatus(fileName string, index int, numPieces int) ([]byte, error) {
	b := make([]byte, numPieces)
	_, err := store.backend.ReadFileMetadataAt(fileName, []FileState{stateDownload}, getPieceStatus(), b, int64(index))
	if IsFileStateError(err) {
		// For files that finished downloading or were pushed, piece status should be done.
		if _, e := store.backend.GetFileStat(fileName, []FileState{stateCache}); e == nil {
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
	_, err := store.backend.WriteFileMetadata(fileName, []FileState{stateUpload}, getStartedAt(), content)
	return err
}

// GetUploadFileStartedAt reads creation file for a new upload file.
func (store *LocalFileStore) GetUploadFileStartedAt(fileName string) ([]byte, error) {
	return store.backend.ReadFileMetadata(fileName, []FileState{stateUpload}, getStartedAt())
}

// DeleteUploadFileStartedAt deletes creation file for a new upload file.
func (store *LocalFileStore) DeleteUploadFileStartedAt(fileName string) error {
	return store.backend.DeleteFileMetadata(fileName, []FileState{stateUpload}, getStartedAt())
}

// SetUploadFileHashState creates and writes hashstate for a upload file.
func (store *LocalFileStore) SetUploadFileHashState(fileName string, content []byte, algorithm string, code string) error {
	_, err := store.backend.WriteFileMetadata(fileName, []FileState{stateUpload}, getHashState(algorithm, code), content)
	return err
}

// GetUploadFileHashState reads hashstate for a upload file.
func (store *LocalFileStore) GetUploadFileHashState(fileName string, algorithm string, code string) ([]byte, error) {
	return store.backend.ReadFileMetadata(fileName, []FileState{stateUpload}, getHashState(algorithm, code))
}

// ListUploadFileHashStatePaths list paths of all hashstates for a upload file.
// This function is not thread-safe.
// TODO: Right now we store metadata with _hashstate, but registry expects /hashstate.
func (store *LocalFileStore) ListUploadFileHashStatePaths(fileName string) ([]string, error) {
	fp, err := store.backend.GetFilePath(fileName, []FileState{stateUpload})
	if err != nil {
		return nil, err
	}

	mtList, err := store.backend.ListFileMetadata(fileName, []FileState{stateUpload})
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, mt := range mtList {
		if re := regexp.MustCompile("_hashstates/\\w+/\\w+$"); re.MatchString(mt.Suffix()) {
			r := strings.NewReplacer("_", "/")
			paths = append(paths, fp+r.Replace(mt.Suffix()))
		}
	}
	return paths, nil
}

// GetUploadFileReader returns a FileReader for a file in upload directory.
func (store *LocalFileStore) GetUploadFileReader(fileName string) (FileReader, error) {
	return store.backend.GetFileReader(fileName, []FileState{stateUpload})
}

// GetCacheFileReader returns a FileReader for a file in cache directory.
func (store *LocalFileStore) GetCacheFileReader(fileName string) (FileReader, error) {
	return store.backend.GetFileReader(fileName, []FileState{stateCache})
}

// GetUploadFileReadWriter returns a FileReadWriter for a file in upload directory.
func (store *LocalFileStore) GetUploadFileReadWriter(fileName string) (FileReadWriter, error) {
	return store.backend.GetFileReadWriter(fileName, []FileState{stateUpload})
}

// GetDownloadFileReadWriter returns a FileReadWriter for a file in download directory.
func (store *LocalFileStore) GetDownloadFileReadWriter(fileName string) (FileReadWriter, error) {
	return store.backend.GetFileReadWriter(fileName, []FileState{stateDownload})
}

// GetDownloadOrCacheFileReader returns a FileReader for a file in download or cache directory.
func (store *LocalFileStore) GetDownloadOrCacheFileReader(fileName string) (FileReader, error) {
	return store.backend.GetFileReader(fileName, []FileState{stateDownload, stateCache})
}

// GetCacheFilePath returns full path of a file in cache directory.
func (store *LocalFileStore) GetCacheFilePath(fileName string) (string, error) {
	return store.backend.GetFilePath(fileName, []FileState{stateCache})
}

// GetCacheFileStat returns a FileInfo of a file in cache directory.
func (store *LocalFileStore) GetCacheFileStat(fileName string) (os.FileInfo, error) {
	return store.backend.GetFileStat(fileName, []FileState{stateCache})
}

// MoveUploadFileToCache moves a file from upload directory to cache directory.
func (store *LocalFileStore) MoveUploadFileToCache(fileName, targetFileName string) error {
	return store.backend.RenameFile(fileName, []FileState{stateUpload}, targetFileName, stateCache)
}

// MoveDownloadFileToCache moves a file from download directory to cache directory.
func (store *LocalFileStore) MoveDownloadFileToCache(fileName string) error {
	return store.backend.MoveFile(fileName, []FileState{stateDownload}, stateCache)
}

// MoveCacheFileToTrash moves a file from cache directory to trash directory.
func (store *LocalFileStore) MoveCacheFileToTrash(fileName string) error {
	return store.backend.MoveFile(fileName, []FileState{stateCache}, stateTrash)
}

// DeleteTrashFile permanently deletes a file from trash directory.
func (store *LocalFileStore) DeleteTrashFile(fileName string) error {
	return store.backend.DeleteFile(fileName, []FileState{stateTrash})
}

// IncrementCacheFileRefCount increments ref count for a file in cache directory.
func (store *LocalFileStore) IncrementCacheFileRefCount(fileName string, states []FileState) (int64, error) {
	return store.backend.IncrementFileRefCount(fileName, []FileState{stateCache})
}

// DecrementCacheFileRefCount decrements ref count for a file in cache directory.
// If ref count reaches 0, it will try to rename it and move it to trash directory.
func (store *LocalFileStore) DecrementCacheFileRefCount(fileName string, states []FileState) (int64, error) {
	refCount, err := store.backend.DecrementFileRefCount(fileName, []FileState{stateCache})
	if err != nil {
		return refCount, err
	}
	// Try rename and move to trash.
	if refCount == 0 {
		err := store.backend.RenameFile(fileName, []FileState{stateCache}, fileName+"."+uuid.Generate().String(), stateTrash)
		if IsRefCountError(err) {
			// It's possible ref count was incremented again, and that's fine.
			return err.(*RefCountError).RefCount, nil
		}
		return 0, err
	}
	return refCount, nil
}
