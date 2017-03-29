package store

import (
	"fmt"
	"os"
	"path"
	"sync"
)

// FileStoreBackend manages all agent files.
type FileStoreBackend interface {
	CreateFile(fileName string, state FileState, len int64) (bool, error)
	SetFileMetadata(fileName string, state FileState, content []byte, mt metadataType, additionalArgs ...interface{}) error
	GetFileMetadata(fileName string, state FileState, data []byte, mt metadataType, additionalArgs ...interface{}) error
	GetFileReader(fileName string, state FileState) (FileReader, error)
	GetFileReadWriter(fileName string, state FileState) (FileReadWriter, error)
	// TODO (@evelynl): move/delet metadata based on metadataType
	MoveFile(fileName string, state, nextState FileState) error
	MoveFileIn(fileName string, state FileState, sourcePath string) error
	MoveFileOut(fileName string, state FileState, targetPath string) error
	DeleteFile(fileName string, state FileState) error
}

// localFileStoreBackend manages all agent files on local disk under a global lock.
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

// SetFileMetadata creates or overwrites metadata assocciate with the file with content
func (backend *localFileStoreBackend) SetFileMetadata(fileName string, state FileState, content []byte, mt metadataType, additionalArgs ...interface{}) error {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, ok := backend.fileMap[fileName]
	if !ok {
		return fmt.Errorf("Failed to set file metadata for %s. File entry does not exist", fileName)
	}

	if fileEntry.GetState() != state {
		return fmt.Errorf("Failed to set file metadata for %s. File state does not match: expected %s but got %s", fileEntry.GetState(), state, fileName)
	}

	// Create metadata file
	err := fileEntry.SetMetadata(mt, content, additionalArgs)
	if err != nil {
		return err
	}
	return nil
}

// GetFileMetadata returns metadata assocciate with the file
func (backend *localFileStoreBackend) GetFileMetadata(fileName string, state FileState, data []byte, mt metadataType, additionalArgs ...interface{}) error {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, ok := backend.fileMap[fileName]
	if !ok {
		return fmt.Errorf("Failed to get file metadata for %s. File entry does not exist", fileName)
	}

	if fileEntry.GetState() != state {
		return fmt.Errorf("Failed to get file metadata for %s. File state does not match: expected %s but got %s", fileEntry.GetState(), state, fileName)
	}

	// Get metadata
	err := fileEntry.GetMetadata(mt, data, additionalArgs)
	if err != nil {
		return err
	}
	return nil
}

// CreateFile creates an empty file with specified size. If file exists, do nothing. Returns if the file is new
func (backend *localFileStoreBackend) CreateFile(fileName string, state FileState, len int64) (bool, error) {
	backend.Lock()
	defer backend.Unlock()

	if _, ok := backend.fileMap[fileName]; ok {
		return false, nil
	}

	targetPath := path.Join(state.GetDirectory(), fileName)

	// Create file.
	f, err := os.Create(targetPath)
	if err != nil {
		return true, err
	}
	defer f.Close()

	// Change size
	err = f.Truncate(len)
	if err != nil {
		return true, err
	}

	backend.fileMap[fileName] = NewLocalFileEntry(fileName, state)
	return true, nil
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

// MoveFileIn moves a file from unmanaged location to file store.
func (backend *localFileStoreBackend) MoveFileIn(fileName string, state FileState, sourcePath string) error {
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

// MoveFileIn moves a file from file store to unmanaged location.
func (backend *localFileStoreBackend) MoveFileOut(fileName string, state FileState, targetPath string) error {
	backend.Lock()
	defer backend.Unlock()

	f, ok := backend.fileMap[fileName]
	if !ok || f.GetState() != state {
		return fmt.Errorf("Cannot find file %s under directory %s", fileName, state.GetDirectory())
	}

	if f.IsOpen() {
		return fmt.Errorf("Cannot remove file %s because it's still open", fileName)
	}

	sourcePath := path.Join(state.GetDirectory(), fileName)
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	delete(backend.fileMap, fileName)
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

	delete(backend.fileMap, fileName)
	return nil
}
