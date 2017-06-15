package store

import (
	"log"
	"os"
	"path"
	"regexp"
	"strings"

	"code.uber.internal/infra/kraken/configuration"

	"github.com/docker/distribution/uuid"
)

// LocalFileStore manages all agent files on local disk.
type LocalFileStore struct {
	uploadBackend        FileStoreBackend
	downloadCacheBackend FileStoreBackend

	trashDir string
}

// NewLocalFileStore initializes and returns a new LocalFileStore object.
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

	return &LocalFileStore{
		downloadCacheBackend: NewLocalFileStoreBackend(),
		uploadBackend:        NewLocalFileStoreBackend(),
		trashDir:             config.TrashDir,
	}
}

// CreateUploadFile creates an empty file in upload directory with specified size.
func (store *LocalFileStore) CreateUploadFile(fileName string, len int64) (bool, error) {
	return store.uploadBackend.CreateFile(fileName, []FileState{}, stateUpload, len)
}

// CreateDownloadFile creates an empty file in download directory with specified size.
func (store *LocalFileStore) CreateDownloadFile(fileName string, len int64) (bool, error) {
	return store.downloadCacheBackend.CreateFile(fileName, []FileState{stateDownload}, stateDownload, len)
}

// WriteDownloadFilePieceStatus creates or overwrites piece status for a new download file.
func (store *LocalFileStore) WriteDownloadFilePieceStatus(fileName string, content []byte) (bool, error) {
	return store.downloadCacheBackend.WriteFileMetadata(fileName, []FileState{stateDownload}, getPieceStatus(), content)
}

// WriteDownloadFilePieceStatusAt update piece status for download file at given index.
func (store *LocalFileStore) WriteDownloadFilePieceStatusAt(fileName string, content []byte, index int) (bool, error) {
	n, err := store.downloadCacheBackend.WriteFileMetadataAt(fileName, []FileState{stateDownload}, getPieceStatus(), content, int64(index))
	if n == 0 {
		return false, err
	}
	return true, err
}

// GetFilePieceStatus reads piece status for a file that's in download or cache dir.
func (store *LocalFileStore) GetFilePieceStatus(fileName string, index int, numPieces int) ([]byte, error) {
	b := make([]byte, numPieces)
	_, err := store.downloadCacheBackend.ReadFileMetadataAt(fileName, []FileState{stateDownload}, getPieceStatus(), b, int64(index))
	if IsFileStateError(err) {
		// For files that finished downloading or were pushed, piece status should be done.
		if _, e := store.downloadCacheBackend.GetFileStat(fileName, []FileState{stateCache}); e == nil {
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
	_, err := store.uploadBackend.WriteFileMetadata(fileName, []FileState{stateUpload}, getStartedAt(), content)
	return err
}

// GetUploadFileStartedAt reads creation file for a new upload file.
func (store *LocalFileStore) GetUploadFileStartedAt(fileName string) ([]byte, error) {
	return store.uploadBackend.ReadFileMetadata(fileName, []FileState{stateUpload}, getStartedAt())
}

// DeleteUploadFileStartedAt deletes creation file for a new upload file.
func (store *LocalFileStore) DeleteUploadFileStartedAt(fileName string) error {
	return store.uploadBackend.DeleteFileMetadata(fileName, []FileState{stateUpload}, getStartedAt())
}

// SetUploadFileHashState creates and writes hashstate for a upload file.
func (store *LocalFileStore) SetUploadFileHashState(fileName string, content []byte, algorithm string, code string) error {
	_, err := store.uploadBackend.WriteFileMetadata(fileName, []FileState{stateUpload}, getHashState(algorithm, code), content)
	return err
}

// GetUploadFileHashState reads hashstate for a upload file.
func (store *LocalFileStore) GetUploadFileHashState(fileName string, algorithm string, code string) ([]byte, error) {
	return store.uploadBackend.ReadFileMetadata(fileName, []FileState{stateUpload}, getHashState(algorithm, code))
}

// ListUploadFileHashStatePaths list paths of all hashstates for a upload file.
// This function is not thread-safe.
// TODO: Right now we store metadata with _hashstate, but registry expects /hashstate.
func (store *LocalFileStore) ListUploadFileHashStatePaths(fileName string) ([]string, error) {
	fp, err := store.uploadBackend.GetFilePath(fileName, []FileState{stateUpload})
	if err != nil {
		return nil, err
	}

	mtList, err := store.uploadBackend.ListFileMetadata(fileName, []FileState{stateUpload})
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
	return store.uploadBackend.GetFileReader(fileName, []FileState{stateUpload})
}

// GetCacheFileReader returns a FileReader for a file in cache directory.
func (store *LocalFileStore) GetCacheFileReader(fileName string) (FileReader, error) {
	return store.downloadCacheBackend.GetFileReader(fileName, []FileState{stateCache})
}

// GetUploadFileReadWriter returns a FileReadWriter for a file in upload directory.
func (store *LocalFileStore) GetUploadFileReadWriter(fileName string) (FileReadWriter, error) {
	return store.uploadBackend.GetFileReadWriter(fileName, []FileState{stateUpload})
}

// GetDownloadFileReadWriter returns a FileReadWriter for a file in download directory.
func (store *LocalFileStore) GetDownloadFileReadWriter(fileName string) (FileReadWriter, error) {
	return store.downloadCacheBackend.GetFileReadWriter(fileName, []FileState{stateDownload})
}

// GetDownloadOrCacheFileReader returns a FileReader for a file in download or cache directory.
func (store *LocalFileStore) GetDownloadOrCacheFileReader(fileName string) (FileReader, error) {
	return store.downloadCacheBackend.GetFileReader(fileName, []FileState{stateDownload, stateCache})
}

// GetCacheFilePath returns full path of a file in cache directory.
func (store *LocalFileStore) GetCacheFilePath(fileName string) (string, error) {
	return store.downloadCacheBackend.GetFilePath(fileName, []FileState{stateCache})
}

// GetCacheFileStat returns a FileInfo of a file in cache directory.
func (store *LocalFileStore) GetCacheFileStat(fileName string) (os.FileInfo, error) {
	return store.downloadCacheBackend.GetFileStat(fileName, []FileState{stateCache})
}

// MoveUploadFileToCache moves a file from upload directory to cache directory.
func (store *LocalFileStore) MoveUploadFileToCache(fileName, targetFileName string) error {
	uploadFilePath, err := store.uploadBackend.GetFilePath(fileName, []FileState{stateUpload})
	if err != nil {
		return err
	}
	_, err = store.downloadCacheBackend.CreateLinkFromFile(targetFileName, []FileState{stateCache}, stateCache, uploadFilePath)
	if err != nil {
		return err
	}
	err = store.uploadBackend.DeleteFile(fileName, []FileState{stateUpload})
	return err
}

// MoveDownloadFileToCache moves a file from download directory to cache directory.
func (store *LocalFileStore) MoveDownloadFileToCache(fileName string) error {
	return store.downloadCacheBackend.MoveFile(fileName, []FileState{stateDownload}, stateCache)
}

// MoveCacheFileToTrash moves a file from cache directory to trash directory, and append a random suffix so there won't be name collision.
func (store *LocalFileStore) MoveCacheFileToTrash(fileName string) error {
	if err := store.downloadCacheBackend.LinkToFile(fileName, []FileState{stateCache}, path.Join(store.trashDir, fileName+"."+uuid.Generate().String())); err != nil {
		return err
	}
	return store.uploadBackend.DeleteFile(fileName, []FileState{stateCache})
}

// DeleteAllTrashFiles permanently deletes all files from trash directory.
// This function is not executed inside global lock, and expects itself to be the only one doing deletion.
func (store *LocalFileStore) DeleteAllTrashFiles() error {
	dir, err := os.Open(store.trashDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, fileName := range names {
		err = os.Remove(path.Join(store.trashDir, fileName))
		if err != nil {
			return err
		}
	}
	return nil
}

// IncrementCacheFileRefCount increments ref count for a file in cache directory.
func (store *LocalFileStore) IncrementCacheFileRefCount(fileName string) (int64, error) {
	return store.downloadCacheBackend.IncrementFileRefCount(fileName, []FileState{stateCache})
}

// DecrementCacheFileRefCount decrements ref count for a file in cache directory.
// If ref count reaches 0, it will try to rename it and move it to trash directory.
func (store *LocalFileStore) DecrementCacheFileRefCount(fileName string) (int64, error) {
	refCount, err := store.downloadCacheBackend.DecrementFileRefCount(fileName, []FileState{stateCache})
	if err != nil {
		return refCount, err
	}
	// Try rename and move to trash.
	if refCount == 0 {
		if err := store.downloadCacheBackend.LinkToFile(fileName, []FileState{stateCache}, path.Join(store.trashDir, fileName+"."+uuid.Generate().String())); err != nil {
			return 0, err
		}
		err := store.uploadBackend.DeleteFile(fileName, []FileState{stateUpload})
		if IsRefCountError(err) {
			// It's possible ref count was incremented again, and that's normal. Abort.
			return err.(*RefCountError).RefCount, nil
		}
		return 0, err
	}
	return refCount, nil
}
