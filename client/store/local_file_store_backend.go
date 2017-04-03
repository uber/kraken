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
	SetFileMetadata(fileName string, states []FileState, data []byte, mt metadataType, additionalArgs ...interface{}) error
	GetFileMetadata(fileName string, states []FileState, data []byte, mt metadataType, additionalArgs ...interface{}) error
	GetFileReader(fileName string, states []FileState) (FileReader, error)
	GetFileReadWriter(fileName string, states []FileState) (FileReadWriter, error)
	// TODO (@yiran): This is only needed when migrating classes to filestore
	GetFilePath(fileName string, states []FileState) (string, error)
	GetFileStat(fileName string, states []FileState) (os.FileInfo, error)
	// TODO (@evelynl): move/delet metadata based on metadataType
	MoveFile(fileName string, states []FileState, goalState FileState) error
	MoveFileIn(fileName string, goalState FileState, sourcePath string) error
	MoveFileOut(fileName string, states []FileState, targetPath string) error
	DeleteFile(fileName string, states []FileState) error
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

// getFileEntry checks if file exists and in one of the desired states, then returns FileEntry.
// Not thread-safe.
func (backend *localFileStoreBackend) getFileEntry(fileName string, states []FileState) (FileEntry, error) {
	fileEntry, exists := backend.fileMap[fileName]
	if exists {
		for _, state := range states {
			if fileEntry.GetState() == state {
				return fileEntry, nil
			}
		}
		return nil, &FileStateError{Op: "get", State: fileEntry.GetState(), Name: fileName, Msg: fmt.Sprintf("Desired states: %v", states)}
	}
	for _, state := range states {
		if _, err := os.Stat(path.Join(state.GetDirectory(), fileName)); err == nil {
			// File exists on disk, load into memory and return new obj.
			fileEntry = NewLocalFileEntry(fileName, state)
			backend.fileMap[fileName] = fileEntry
			exists = true
			return fileEntry, nil
		}
	}
	return nil, &os.PathError{Op: "get", Path: fileName, Err: os.ErrNotExist}
}

// CreateFile creates an empty file with specified size. If file exists, do nothing. Returns true if the file is new.
func (backend *localFileStoreBackend) CreateFile(fileName string, state FileState, len int64) (bool, error) {
	backend.Lock()
	defer backend.Unlock()

	_, err := backend.getFileEntry(fileName, []FileState{state})
	if err == nil || IsFileStateError(err) {
		return false, err
	}

	targetPath := path.Join(state.GetDirectory(), fileName)

	// Create file.
	f, err := os.Create(targetPath)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Change size
	err = f.Truncate(len)
	if err != nil {
		return false, err
	}

	backend.fileMap[fileName] = NewLocalFileEntry(fileName, state)
	return true, nil
}

// SetFileMetadata creates or overwrites metadata assocciate with the file with content
func (backend *localFileStoreBackend) SetFileMetadata(fileName string, states []FileState, data []byte, mt metadataType, additionalArgs ...interface{}) error {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return err
	}

	// Create metadata file
	err = fileEntry.SetMetadata(mt, data, additionalArgs)
	if err != nil {
		return err
	}
	return nil
}

// GetFileMetadata returns metadata assocciate with the file
func (backend *localFileStoreBackend) GetFileMetadata(fileName string, states []FileState, data []byte, mt metadataType, additionalArgs ...interface{}) error {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return err
	}

	// Get metadata
	err = fileEntry.GetMetadata(mt, data, additionalArgs)
	if err != nil {
		return err
	}
	return nil
}

// GetFileReader returns a FileReader object for read operations.
func (backend *localFileStoreBackend) GetFileReader(fileName string, states []FileState) (FileReader, error) {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}

	return fileEntry.GetFileReader()
}

// GetFileReadWriter returns a FileReadWriter object for read/write operations.
func (backend *localFileStoreBackend) GetFileReadWriter(fileName string, states []FileState) (FileReadWriter, error) {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}

	return fileEntry.GetFileReadWriter()
}

// GetFilePath returns full path for a file.
func (backend *localFileStoreBackend) GetFilePath(fileName string, states []FileState) (string, error) {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return "", err
	}

	return fileEntry.GetPath(), nil
}

// GetFileStat returns FileInfo for a file.
func (backend *localFileStoreBackend) GetFileStat(fileName string, states []FileState) (os.FileInfo, error) {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}

	return fileEntry.Stat()
}

// MoveFile moves a file to a different directory and updates its state accordingly.
func (backend *localFileStoreBackend) MoveFile(fileName string, states []FileState, goalState FileState) error {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, append(states, goalState))
	if err != nil {
		return err
	}
	if fileEntry.GetState() == goalState {
		return nil
	}

	if fileEntry.IsOpen() {
		return fmt.Errorf("Cannot remove file %s because it's still open", fileName)
	}

	sourcePath := path.Join(fileEntry.GetState().GetDirectory(), fileName)
	targetPath := path.Join(goalState.GetDirectory(), fileName)
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	backend.fileMap[fileName].SetState(goalState)
	return nil
}

// MoveFileIn moves a file from unmanaged location to file store.
func (backend *localFileStoreBackend) MoveFileIn(fileName string, goalState FileState, sourcePath string) error {
	backend.Lock()
	defer backend.Unlock()

	_, err := backend.getFileEntry(fileName, []FileState{goalState})
	if err != nil {
		return err
	}

	targetPath := path.Join(goalState.GetDirectory(), fileName)
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	backend.fileMap[fileName] = NewLocalFileEntry(fileName, goalState)
	return nil
}

// MoveFileIn moves a file from file store to unmanaged location.
func (backend *localFileStoreBackend) MoveFileOut(fileName string, states []FileState, targetPath string) error {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return err
	}

	if fileEntry.IsOpen() {
		return fmt.Errorf("Cannot remove file %s because it's still open", fileName)
	}

	sourcePath := path.Join(fileEntry.GetState().GetDirectory(), fileName)
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	delete(backend.fileMap, fileName)
	return nil
}

// DeleteFile removes a file from disk.
func (backend *localFileStoreBackend) DeleteFile(fileName string, states []FileState) error {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return err
	}

	if fileEntry.IsOpen() {
		return fmt.Errorf("Cannot remove file %s because it's still open", fileName)
	}

	if err := os.Remove(path.Join(fileEntry.GetState().GetDirectory(), fileName)); err != nil {
		return err
	}

	delete(backend.fileMap, fileName)
	return nil
}
