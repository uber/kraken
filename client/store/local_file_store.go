package store

import (
	"fmt"
	"os"
	"path"
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
func (store *LocalFileStore) Add(fileName string) error {
	store.Lock()
	defer store.Unlock()

	if _, ok := store.fileMap[fileName]; ok {
		return fmt.Errorf("Cannot add file %s because it already exists", fileName)
	}

	sourcePath := path.Join(store.sourceRoot, fileName)
	storePath := path.Join(store.storeRoot, fileName)
	if err := os.Rename(sourcePath, storePath); err != nil {
		return err
	}

	store.fileMap[fileName] = NewLocalFile(storePath, fileName)
	return nil
}

// Get returns a pointer to a LocalFile object that implements SectionReader and Closer interfaces.
func (store *LocalFileStore) Get(fileName string) (*LocalFileReader, error) {
	store.Lock()
	defer store.Unlock()

	f, ok := store.fileMap[fileName]
	if !ok {
		return nil, fmt.Errorf("File %s doesn't exist", fileName)
	}

	return NewLocalFileReader(f)
}

// Delete removes a file from store.
func (store *LocalFileStore) Delete(fileName string) error {
	store.Lock()
	defer store.Unlock()

	f, ok := store.fileMap[fileName]
	if !ok {
		return fmt.Errorf("File %s doesn't exist", fileName)
	}

	if f.isOpen() {
		return fmt.Errorf("Cannot remove file %s because it's still open", f.name)
	}

	trashPath := path.Join(store.trashRoot, f.name)
	if err := os.Rename(f.path, trashPath); err != nil {
		return err
	}
	return nil
}
