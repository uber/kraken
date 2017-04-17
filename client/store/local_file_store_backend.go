package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
)

// FileStoreBackend manages all agent files.
type FileStoreBackend interface {
	CreateFile(fileName string, acceptedStates []FileState, createState FileState, len int64) (bool, error)

	GetFilePath(fileName string, states []FileState) (string, error)
	GetFileStat(fileName string, states []FileState) (os.FileInfo, error)

	ReadFileMetadata(fileName string, states []FileState, mt MetadataType) ([]byte, error)
	WriteFileMetadata(fileName string, states []FileState, mt MetadataType, data []byte) (bool, error)
	ReadFileMetadataAt(fileName string, states []FileState, mt MetadataType, b []byte, off int64) (int, error)
	WriteFileMetadataAt(fileName string, states []FileState, mt MetadataType, b []byte, off int64) (int, error)
	DeleteFileMetadata(fileName string, states []FileState, mt MetadataType) error
	ListFileMetadata(fileName string, states []FileState) ([]MetadataType, error)

	GetFileReader(fileName string, states []FileState) (FileReader, error)
	GetFileReadWriter(fileName string, states []FileState) (FileReadWriter, error)

	MoveFile(fileName string, states []FileState, goalState FileState) error
	RenameFile(fileName string, states []FileState, targetFileName string, goalState FileState) error
	MoveFileIn(fileName string, goalState FileState, sourcePath string) error
	MoveFileOut(fileName string, states []FileState, targetPath string) error
	DeleteFile(fileName string, states []FileState) error

	IncrementFileRefCount(fileName string, states []FileState) (int64, error)
	DecrementFileRefCount(fileName string, states []FileState) (int64, error)
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
		fp := path.Join(state.GetDirectory(), fileName)
		if _, err := os.Stat(fp); err == nil {
			// File exists on disk
			fileEntry = NewLocalFileEntry(fileName, state)

			// Load metadata
			paths, err := filepath.Glob(fp + "*")
			if err != nil {
				return nil, err
			}
			for _, path := range paths {
				// Glob could return the data file itself, and directories. Verify it's actually a metadata file here.
				mt := getMetadataType(path)
				if mt != nil {
					_, err = fileEntry.ReadMetadata(mt)
					if err != nil {
						return nil, err
					}
				}
			}

			// Load file entry into in-memory map and return new obj
			backend.fileMap[fileName] = fileEntry
			exists = true
			return fileEntry, nil
		}
	}
	return nil, &os.PathError{Op: "get", Path: fileName, Err: os.ErrNotExist}
}

// CreateFile creates an empty file with specified size. If file exists, do nothing. Returns true if the file is new.
func (backend *localFileStoreBackend) CreateFile(fileName string, acceptedStates []FileState, createState FileState, len int64) (bool, error) {
	backend.Lock()
	defer backend.Unlock()

	_, err := backend.getFileEntry(fileName, append(acceptedStates, createState))
	if err == nil || IsFileStateError(err) {
		return false, err
	}

	targetPath := path.Join(createState.GetDirectory(), fileName)

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

	backend.fileMap[fileName] = NewLocalFileEntry(fileName, createState)
	return true, nil
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

// ReadFileMetadata returns metadata assocciate with the file
func (backend *localFileStoreBackend) ReadFileMetadata(fileName string, states []FileState, mt MetadataType) ([]byte, error) {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}

	// Get metadata
	return fileEntry.ReadMetadata(mt)
}

// WriteFileMetadata creates or overwrites metadata assocciate with the file with content
func (backend *localFileStoreBackend) WriteFileMetadata(fileName string, states []FileState, mt MetadataType, data []byte) (bool, error) {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return false, err
	}

	// Create/Write metadata file
	return fileEntry.WriteMetadata(mt, data)
}

// ReadFileMetadataAt returns metadata assocciate with the file
func (backend *localFileStoreBackend) ReadFileMetadataAt(fileName string, states []FileState, mt MetadataType, b []byte, off int64) (int, error) {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	// Read metadata
	return fileEntry.ReadMetadataAt(mt, b, off)
}

// WriteFileMetadataAt overwrites metadata assocciate with the file with content.
func (backend *localFileStoreBackend) WriteFileMetadataAt(fileName string, states []FileState, mt MetadataType, b []byte, off int64) (int, error) {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	// Write metadata file
	return fileEntry.WriteMetadataAt(mt, b, off)
}

// DeleteFileMetadata deletes metadata of the specified type for a file.
func (backend *localFileStoreBackend) DeleteFileMetadata(fileName string, states []FileState, mt MetadataType) error {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return err
	}

	return fileEntry.DeleteMetadata(mt)
}

// ListFileMetadata returns a list of all metadata for a file.
func (backend *localFileStoreBackend) ListFileMetadata(fileName string, states []FileState) ([]MetadataType, error) {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}

	return fileEntry.ListMetadata(), nil
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

// MoveFile moves a file to a different directory and updates its state accordingly.
func (backend *localFileStoreBackend) MoveFile(fileName string, states []FileState, goalState FileState) error {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, append(states, goalState))
	if err != nil {
		return err
	}
	if fileEntry.GetState() == goalState {
		return &os.PathError{Op: "move", Path: fileName, Err: os.ErrExist}
	}
	refCount, err := fileEntry.GetRefCount()
	if err != nil {
		return err
	}
	if refCount > 0 {
		return &RefCountError{Op: "move", State: fileEntry.GetState(), Name: fileName, RefCount: refCount, Msg: fmt.Sprintf("File still referenced")}
	}
	if fileEntry.IsOpen() {
		// TODO: set goal state.
		return fmt.Errorf("Cannot remove file %s because it's still open", fileName)
	}

	// Copy metadata first. Use copy instead of move here, so any failure would be recoverable.
	var sourceMetadataPaths []string
	for _, mt := range fileEntry.ListMetadata() {
		b, err := fileEntry.ReadMetadata(mt)
		if err != nil {
			return err
		}
		sourceMetadataPath := path.Join(fileEntry.GetState().GetDirectory(), fileName+mt.Suffix())
		sourceMetadataPaths = append(sourceMetadataPaths, sourceMetadataPath)

		if mt.IsValidState(goalState) {
			targetMetadataPath := path.Join(goalState.GetDirectory(), fileName+mt.Suffix())

			err = os.MkdirAll(path.Dir(targetMetadataPath), 0755)
			if err != nil {
				return err
			}
			if err = ioutil.WriteFile(targetMetadataPath, b, 0755); err != nil {
				return err
			}
		}
	}

	// Move data file, change state accordingly.
	sourcePath := fileEntry.GetPath()
	targetPath := path.Join(goalState.GetDirectory(), fileName)
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}
	backend.fileMap[fileName].SetState(goalState)

	// Remove old metadata files, ignore error.
	for _, sourceMetadataPath := range sourceMetadataPaths {
		os.RemoveAll(sourceMetadataPath)
	}

	return nil
}

// MoveFile moves a file to a different directory and also renames it.
func (backend *localFileStoreBackend) RenameFile(fileName string, states []FileState, targetFileName string, goalState FileState) error {
	backend.Lock()
	defer backend.Unlock()

	_, err := backend.getFileEntry(targetFileName, []FileState{goalState})
	if err == nil {
		return &os.PathError{Op: "rename", Path: targetFileName, Err: os.ErrExist}
	}
	if IsFileStateError(err) {
		return err
	}

	fileEntry, err := backend.getFileEntry(fileName, append(states))
	if err != nil {
		return err
	}
	refCount, err := fileEntry.GetRefCount()
	if err != nil {
		return err
	}
	if refCount > 0 {
		return &RefCountError{Op: "move", State: fileEntry.GetState(), Name: fileName, RefCount: refCount, Msg: fmt.Sprintf("File still referenced")}
	}
	if fileEntry.IsOpen() {
		// TODO: set goal state.
		return fmt.Errorf("Cannot remove file %s because it's still open", fileName)
	}

	// Get list of metadata.
	var sourceMetadataPaths []string
	for _, mt := range fileEntry.ListMetadata() {
		sourceMetadataPath := path.Join(fileEntry.GetState().GetDirectory(), fileName+mt.Suffix())
		sourceMetadataPaths = append(sourceMetadataPaths, sourceMetadataPath)
	}

	// Move data file.
	sourcePath := path.Join(fileEntry.GetState().GetDirectory(), fileName)
	targetPath := path.Join(goalState.GetDirectory(), targetFileName)
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}
	backend.fileMap[targetFileName] = NewLocalFileEntry(targetFileName, goalState)
	delete(backend.fileMap, fileName)

	// Remove old metadata files, ignore error.
	for _, sourceMetadataPath := range sourceMetadataPaths {
		os.RemoveAll(sourceMetadataPath)
	}

	return nil
}

// MoveFileIn moves a file from unmanaged location to file store.
func (backend *localFileStoreBackend) MoveFileIn(fileName string, goalState FileState, sourcePath string) error {
	backend.Lock()
	defer backend.Unlock()

	_, err := backend.getFileEntry(fileName, []FileState{goalState})
	if err == nil {
		return &os.PathError{Op: "move", Path: fileName, Err: os.ErrExist}
	}
	if IsFileStateError(err) {
		return err
	}

	targetPath := path.Join(goalState.GetDirectory(), fileName)
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	backend.fileMap[fileName] = NewLocalFileEntry(fileName, goalState)
	return nil
}

// MoveFileOut moves a file from file store to unmanaged location.
func (backend *localFileStoreBackend) MoveFileOut(fileName string, states []FileState, targetPath string) error {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return err
	}
	refCount, err := fileEntry.GetRefCount()
	if err != nil {
		return err
	}
	if refCount > 0 {
		return &RefCountError{Op: "move", State: fileEntry.GetState(), Name: fileName, RefCount: refCount, Msg: fmt.Sprintf("File still referenced")}
	}
	if fileEntry.IsOpen() {
		// TODO: set goal state?
		return fmt.Errorf("Cannot remove file %s because it's still open", fileName)
	}

	// Get list of metadata.
	var sourceMetadataPaths []string
	for _, mt := range fileEntry.ListMetadata() {
		sourceMetadataPath := path.Join(fileEntry.GetState().GetDirectory(), fileName+mt.Suffix())
		sourceMetadataPaths = append(sourceMetadataPaths, sourceMetadataPath)
	}

	// Move data file.
	sourcePath := path.Join(fileEntry.GetState().GetDirectory(), fileName)
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}
	delete(backend.fileMap, fileName)

	// Remove old metadata files, ignore error.
	for _, sourceMetadataPath := range sourceMetadataPaths {
		os.RemoveAll(sourceMetadataPath)
	}

	return nil
}

// DeleteFile removes a file from disk.
// TODO: delete metadata files.
func (backend *localFileStoreBackend) DeleteFile(fileName string, states []FileState) error {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return err
	}
	refCount, err := fileEntry.GetRefCount()
	if err != nil {
		return err
	}
	if refCount > 0 {
		return &RefCountError{Op: "move", State: fileEntry.GetState(), Name: fileName, RefCount: refCount, Msg: fmt.Sprintf("File still referenced")}
	}
	if fileEntry.IsOpen() {
		// TODO: set goal state?
		return fmt.Errorf("Cannot remove file %s because it's still open", fileName)
	}

	// Get list of metadata.
	var sourceMetadataPaths []string
	for _, mt := range fileEntry.ListMetadata() {
		sourceMetadataPath := path.Join(fileEntry.GetState().GetDirectory(), fileName+mt.Suffix())
		sourceMetadataPaths = append(sourceMetadataPaths, sourceMetadataPath)
	}

	// Remove from map.
	delete(backend.fileMap, fileName)
	// Remove data file in a separate go routing, so it won't be blocking.
	go func() {
		os.Remove(path.Join(fileEntry.GetState().GetDirectory(), fileName))
	}()

	// Remove old metadata files, ignore error.
	for _, sourceMetadataPath := range sourceMetadataPaths {
		os.RemoveAll(sourceMetadataPath)
	}

	return nil
}

func (backend *localFileStoreBackend) IncrementFileRefCount(fileName string, states []FileState) (int64, error) {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	return fileEntry.IncrementRefCount()
}

func (backend *localFileStoreBackend) DecrementFileRefCount(fileName string, states []FileState) (int64, error) {
	backend.Lock()
	defer backend.Unlock()

	fileEntry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	return fileEntry.DecrementRefCount()
}
