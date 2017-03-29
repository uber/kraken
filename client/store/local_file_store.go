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

// CreateDownloadFile create an empty file in download directory with specified size.
func (store *LocalFileStore) CreateDownloadFile(fileName string, len int64) (bool, error) {
	new, err := store.backend.CreateFile(fileName, stateDownload, len)
	if err != nil {
		return new, err
	}
	return new, nil
}

// SetDownloadFilePieceStatus create and initializes piece status for a new download file
func (store *LocalFileStore) SetDownloadFilePieceStatus(fileName string, content []byte) error {
	return store.backend.SetFileMetadata(fileName, stateDownload, content, pieceStatus)
}

// GetDownloadFilePieceStatus create and initializes piece status for a new download file
func (store *LocalFileStore) GetDownloadFilePieceStatus(fileName string, content []byte) error {
	return store.backend.GetFileMetadata(fileName, stateDownload, content, pieceStatus)
}

// SetDownloadFileStartedAt create and writes the creation file for a new download file
func (store *LocalFileStore) SetDownloadFileStartedAt(fileName string, content []byte) error {
	return store.backend.SetFileMetadata(fileName, stateDownload, content, startedAt)
}

// GetDownloadFileStartedAt create and writes the creation file for a new download file
func (store *LocalFileStore) GetDownloadFileStartedAt(fileName string, content []byte) error {
	return store.backend.GetFileMetadata(fileName, stateDownload, content, startedAt)
}

// SetDownloadFileHashStates creates and writes the hashstate for a downloading file
func (store *LocalFileStore) SetDownloadFileHashStates(fileName string, content []byte, algorithm string, code string) error {
	return store.backend.SetFileMetadata(fileName, stateDownload, content, hashStates, algorithm, code)
}

// GetDownloadFileHashStates creates and writes the hashstate for a downloading file
func (store *LocalFileStore) GetDownloadFileHashStates(fileName string, content []byte, algorithm string, code string) error {
	return store.backend.GetFileMetadata(fileName, stateDownload, content, hashStates, algorithm, code)
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
