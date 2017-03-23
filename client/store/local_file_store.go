package store

import "code.uber.internal/infra/kraken/configuration"

// LocalFileStore manages all agent files on local disk.
type LocalFileStore struct {
	backend FileStoreBackend
}

// NewLocalFileStore initializes and returns a new FileStoreBackend object.
func NewLocalFileStore(config *configuration.Config) *LocalFileStore {
	_localFileStateLookup.register(stateCache, config.CacheDir)
	_localFileStateLookup.register(stateDownload, config.DownloadDir)
	_localFileStateLookup.register(stateTrash, config.TrashDir)

	return &LocalFileStore{
		backend: NewLocalFileStoreBackend(),
	}
}

// CreateEmptyDownloadFile create an empty file in download directory with specified size.
func (store *LocalFileStore) CreateEmptyDownloadFile(fileName string, len int64) error {
	return store.backend.CreateEmptyFile(fileName, stateDownload, len)
}

// GetCacheFileReader returns a FileReader for a file in cache directory.
func (store *LocalFileStore) GetCacheFileReader(fileName string) (FileReader, error) {
	return store.backend.GetFileReader(fileName, stateCache)
}

// GetDownloadFileReadWriter returns a FileReadWriter for a file in download directory.
func (store *LocalFileStore) GetDownloadFileReadWriter(fileName string) (FileReadWriter, error) {
	return store.backend.GetFileReadWriter(fileName, stateDownload)
}

// MoveFileToCache moves a file from download directory to cache directory.
func (store *LocalFileStore) MoveFileToCache(fileName string) error {
	return store.backend.MoveFile(fileName, stateDownload, stateCache)
}

// MoveFileToTrash moves a file from cache directory to trash directory.
func (store *LocalFileStore) MoveFileToTrash(fileName string) error {
	return store.backend.MoveFile(fileName, stateCache, stateTrash)
}

// DeleteTrashFile permanently deletes a file from trash directory.
func (store *LocalFileStore) DeleteTrashFile(fileName string) error {
	return store.backend.DeleteFile(fileName, stateTrash)
}
