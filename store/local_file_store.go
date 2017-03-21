package store

import (
	"fmt"
	"os"
	"sync"

	"code.uber.internal/infra/kraken/configuration"
)

// LocalFileStore manages all downloaded files on local file system.
type LocalFileStore struct {
	sync.Mutex

	storeRoot  string
	sourceRoot string
	trashRoot  string
	fileMap    map[string]*LocalFile
}

// NewLocalFileStore returns a pointer to a new LocalFileStore object.
func NewLocalFileStore(config *configuration.Config) *LocalFileStore {
	return &LocalFileStore{
		storeRoot:  config.CacheDir,
		sourceRoot: config.DownloadDir,
		trashRoot:  config.TrashDir,
		fileMap:    make(map[string]*LocalFile),
	}
}

// Add adds a new file to store.
func (store *LocalFileStore) Add(sourceDir, fileName string) (*LocalFile, error) {
	store.Lock()
	defer store.Unlock()

	if _, ok := store.fileMap[fileName]; ok {
		return nil, fmt.Errorf("Cannot add file %s because it already exists", fileName)
	}

	sourcePath := store.sourceRoot + fileName
	storePath := store.storeRoot + fileName
	if err := os.Rename(sourcePath, storePath); err != nil {
		return nil, err
	}

	localFile := NewLocalFile(storePath, fileName)
	store.fileMap[fileName] = localFile
	return localFile, nil
}

// Get returns a pointer to a LocalFile object that implements SectionReader and Closer interfaces.
func (store *LocalFileStore) Get(fileName string) (*LocalFile, error) {
	store.Lock()
	defer store.Unlock()

	f, ok := store.fileMap[fileName]
	if !ok {
		return nil, fmt.Errorf("File %s doesn't exist", fileName)
	}

	err := f.open()
	return f, err
}

// Delete removes a file from store.
func (store *LocalFileStore) Delete(fileName string) error {
	store.Lock()
	defer store.Unlock()

	f, ok := store.fileMap[fileName]
	if !ok {
		return fmt.Errorf("File %s doesn't exist", fileName)
	}

	if open := f.isOpen(); !open {
		return fmt.Errorf("Cannot remove file %s because it's still open", f.name)
	}

	trashPath := store.trashRoot + f.name
	if err := os.Rename(f.path, trashPath); err != nil {
		return err
	}
	return nil
}
