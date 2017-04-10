package store

import (
	"log"
	"os"

	"code.uber.internal/infra/kraken/configuration"
)

// LocalFileStore manages all agent files on local disk.
type LocalFileStore struct {
	backend FileStoreBackend
}

// NewLocalFileStore initializes and returns a new FileStoreBackend object.
func NewLocalFileStore(config *configuration.Config) *LocalFileStore {
	// init all directories
	// upload
	os.RemoveAll(config.UploadDir)
	err := os.MkdirAll(config.UploadDir, 0755)
	if err != nil {
		log.Fatal(err)
	}

	// download
	os.RemoveAll(config.DownloadDir)
	err = os.MkdirAll(config.DownloadDir, 0755)
	if err != nil {
		log.Fatal(err)
	}

	// cache
	// we do not want to remove all existing files in cache directory
	// for the sake of restart
	err = os.MkdirAll(config.CacheDir, 0755)
	if err != nil {
		log.Fatal(err)
	}

	// trash
	os.RemoveAll(config.TrashDir)
	err = os.MkdirAll(config.TrashDir, 0755)
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

// SetUploadFileHashStates creates and writes hashstate for a upload file.
func (store *LocalFileStore) SetUploadFileHashStates(fileName string, content []byte, algorithm string, code string) error {
	_, err := store.backend.WriteFileMetadata(fileName, []FileState{stateUpload}, getHashState(algorithm, code), content)
	return err
}

// GetUploadFileHashStates reads hashstate for a upload file.
func (store *LocalFileStore) GetUploadFileHashStates(fileName string, algorithm string, code string) ([]byte, error) {
	return store.backend.ReadFileMetadata(fileName, []FileState{stateUpload}, getHashState(algorithm, code))
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
