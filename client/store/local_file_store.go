package store

import (
	"os"

	"code.uber.internal/infra/kraken/configuration"
)

// LocalFileStore manages all agent files on local disk.
type LocalFileStore struct {
	backend FileStoreBackend
}

// NewLocalFileStore initializes and returns a new FileStoreBackend object.
func NewLocalFileStore(config *configuration.Config) *LocalFileStore {
	registerFileState(stateUpload, config.UploadDir)
	registerFileState(stateDownload, config.DownloadDir)
	registerFileState(stateCache, config.CacheDir)
	registerFileState(stateTrash, config.TrashDir)

	return &LocalFileStore{
		backend: NewLocalFileStoreBackend(),
	}
}

// CreateUploadFile create an empty file in upload directory with specified size.
func (store *LocalFileStore) CreateUploadFile(fileName string, len int64) (bool, error) {
	return store.backend.CreateFile(fileName, stateUpload, len)
}

// CreateDownloadFile create an empty file in download directory with specified size.
func (store *LocalFileStore) CreateDownloadFile(fileName string, len int64) (bool, error) {
	return store.backend.CreateFile(fileName, stateDownload, len)
}

// SetDownloadFilePieceStatus create and initializes piece status for a new download file
func (store *LocalFileStore) SetDownloadFilePieceStatus(fileName string, content []byte) error {
	return store.backend.SetFileMetadata(fileName, []FileState{stateDownload}, content, pieceStatus)
}

// GetDownloadFilePieceStatus create and initializes piece status for a new download file
func (store *LocalFileStore) GetDownloadFilePieceStatus(fileName string, content []byte) error {
	return store.backend.GetFileMetadata(fileName, []FileState{stateDownload}, content, pieceStatus)
}

// SetDownloadFileStartedAt create and writes the creation file for a new download file
func (store *LocalFileStore) SetDownloadFileStartedAt(fileName string, content []byte) error {
	return store.backend.SetFileMetadata(fileName, []FileState{stateDownload}, content, startedAt)
}

// GetDownloadFileStartedAt create and writes the creation file for a new download file
func (store *LocalFileStore) GetDownloadFileStartedAt(fileName string, content []byte) error {
	return store.backend.GetFileMetadata(fileName, []FileState{stateDownload}, content, startedAt)
}

// SetDownloadFileHashStates creates and writes the hashstate for a downloading file
func (store *LocalFileStore) SetDownloadFileHashStates(fileName string, content []byte, algorithm string, code string) error {
	return store.backend.SetFileMetadata(fileName, []FileState{stateDownload}, content, hashStates, algorithm, code)
}

// GetDownloadFileHashStates creates and writes the hashstate for a downloading file
func (store *LocalFileStore) GetDownloadFileHashStates(fileName string, content []byte, algorithm string, code string) error {
	return store.backend.GetFileMetadata(fileName, []FileState{stateDownload}, content, hashStates, algorithm, code)
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
