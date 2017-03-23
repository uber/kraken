package store

import (
	"fmt"
	"os"
	"path"
	"sync"
)

// FileStoreBackend manages all agent files.
type FileStoreBackend interface {
	CreateEmptyFile(fileName string, state FileState, len int64) error
	CreateFile(fileName string, state FileState, sourcePath string) error
	GetFileReader(fileName string, state FileState) (FileReader, error)
	GetFileReadWriter(fileName string, state FileState) (FileReadWriter, error)
	MoveFile(fileName string, state, nextState FileState) error
	DeleteFile(fileName string, state FileState) error
}

// localFileStoreBackend manages files under a global lock.
type localFileStoreBackend struct {
	sync.Mutex

	fileMap map[string]FileEntry
}

// NewLocalFileStoreBackend initializes and returns a new FileStoreBackend object.
func NewLocalFileStoreBackend() FileStoreBackend {
	return &localFileStoreBackend{
		fileMap: make(map[string]FileEntry),
	}
}

// CreateEmptyFile creates an empty file with specified size.
func (backend *localFileStoreBackend) CreateEmptyFile(fileName string, state FileState, len int64) error {
	backend.Lock()
	defer backend.Unlock()

	if _, ok := backend.fileMap[fileName]; ok {
		return fmt.Errorf("Cannot add file %s because it already exists", fileName)
	}

	targetPath := path.Join(state.GetDirectory(), fileName)

	// Create file.
	f, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Change size
	err = f.Truncate(len)
	if err != nil {
		return err
	}

	backend.fileMap[fileName] = NewLocalFileEntry(fileName, state)
	return nil
}

// CreateFile add a new file to storage by moving an unmanaged file from specified location.
func (backend *localFileStoreBackend) CreateFile(fileName string, state FileState, sourcePath string) error {
	backend.Lock()
	defer backend.Unlock()

	if _, ok := backend.fileMap[fileName]; ok {
		return fmt.Errorf("Cannot add file %s because it already exists", fileName)
	}

	targetPath := path.Join(state.GetDirectory(), fileName)
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	backend.fileMap[fileName] = NewLocalFileEntry(fileName, state)
	return nil
}

// GetFileReader returns a FileReader object for read operations.
func (backend *localFileStoreBackend) GetFileReader(fileName string, state FileState) (FileReader, error) {
	backend.Lock()
	defer backend.Unlock()

	f, ok := backend.fileMap[fileName]
	if !ok || f.GetState() != state {
		return nil, fmt.Errorf("Cannot find file %s under directory %s", fileName, state.GetDirectory())
	}

	return f.GetFileReader()
}

// GetFileReadWriter returns a FileReadWriter object for read/write operations.
func (backend *localFileStoreBackend) GetFileReadWriter(fileName string, state FileState) (FileReadWriter, error) {
	backend.Lock()
	defer backend.Unlock()

	f, ok := backend.fileMap[fileName]
	if !ok || f.GetState() != state {
		return nil, fmt.Errorf("Cannot find file %s under directory %s", fileName, state.GetDirectory())
	}

	return f.GetFileReadWriter()
}

// MoveFile moves a file to a different directory and updates its state accordingly.
func (backend *localFileStoreBackend) MoveFile(fileName string, state, nextState FileState) error {
	backend.Lock()
	defer backend.Unlock()

	f, ok := backend.fileMap[fileName]
	if ok && f.GetState() == nextState {
		return nil
	}
	if !ok || f.GetState() != state {
		return fmt.Errorf("Cannot find file %s under directory %s", fileName, state.GetDirectory())
	}

	sourcePath := path.Join(state.GetDirectory(), fileName)
	targetPath := path.Join(nextState.GetDirectory(), fileName)
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	backend.fileMap[fileName].SetState(nextState)
	return nil
}

// DeleteFile removes a file from disk.
func (backend *localFileStoreBackend) DeleteFile(fileName string, state FileState) error {
	backend.Lock()
	defer backend.Unlock()

	f, ok := backend.fileMap[fileName]
	if !ok || f.GetState() != state {
		return fmt.Errorf("Cannot find file %s under directory %s", fileName, state.GetDirectory())
	}

	if f.IsOpen() {
		return fmt.Errorf("Cannot remove file %s because it's still open", fileName)
	}

	if err := os.Remove(path.Join(state.GetDirectory(), fileName)); err != nil {
		return err
	}
	return nil
}
