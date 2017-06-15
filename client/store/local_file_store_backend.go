package store

import (
	"fmt"
	"os"

	"golang.org/x/sync/syncmap"
)

// localFileStoreBackend manages all agent files on local disk.
// Read/Write operation should access data in this order:
//   map load -> file lock -> verify not deleted -> map load/store -> file/metadata change -> file unlock
// Delete opereration should access data in this order:
//   map load -> file lock -> verify not deleted -> file/metadata change -> delete from map -> file unlock
type localFileStoreBackend struct {
	fileMap *syncmap.Map
}

// NewLocalFileStoreBackend initializes and returns a new FileStoreBackend object.
func NewLocalFileStoreBackend() FileStoreBackend {
	return &localFileStoreBackend{
		fileMap: &syncmap.Map{},
	}
}

// getFileEntry is a helper function that returns file entry from map.
// If the file is not in map, it tries to reload from disk.
// It doesn't verify state.
func (backend *localFileStoreBackend) getFileEntry(fileName string, states []FileState) (FileEntry, error) {
	var fileEntry FileEntry
	var err error
	entry, exists := backend.fileMap.Load(fileName)
	if exists {
		fileEntry = entry.(FileEntry)
	} else {
		// Check if file exists on disk
		for _, state := range states {
			fileEntry, err = ReloadLocalFileEntry(fileName, state)
			if err == nil {
				// Try to store file entry into memory.
				// It's possible the entry exists now, in that case just return existing obj.
				entry, _ := backend.fileMap.LoadOrStore(fileName, fileEntry)
				fileEntry = entry.(FileEntry)
				exists = true
			}
		}
	}

	if !exists {
		return nil, &os.PathError{Op: "get", Path: fileName, Err: os.ErrNotExist}
	}

	return fileEntry, nil
}

// CreateFile creates an empty file with specified size.
// If file exists and is in one of the acceptable states, do nothing. Returns true if the file is new.
func (backend *localFileStoreBackend) CreateFile(fileName string, states []FileState, targetState FileState, len int64) (bool, error) {
	// Verify if file exists in memory or on disk.
	_, err := backend.getFileEntry(fileName, states)
	if !os.IsNotExist(err) {
		return false, err // including nil
	}

	// Create new file entry.
	entry := NewLocalFileEntry(fileName, targetState)

	mapLoadOrStore := func() (FileEntry, error) {
		v, loaded := backend.fileMap.LoadOrStore(fileName, entry)
		if loaded {
			loadedEntry := v.(FileEntry)
			loadedState, err := loadedEntry.GetState(nil) // It's another object, so it's ok to call functions that calls Lock()
			if err != nil {
				return nil, err
			}
			for _, state := range states {
				if loadedState == state {
					return loadedEntry, nil // File already exists in one of acceptable states.
				}
			}
			return nil, &FileStateError{Op: "create", State: loadedState, Name: fileName, Msg: fmt.Sprintf("Desired states: %v", states)}
		}
		return nil, nil
	}

	// Create file on disk.
	return entry.Create(states, targetState, len, mapLoadOrStore)
}

// CreateLinkFromFile create a hardlink of a file from unmanaged location to file store.
// If file exists and is in one of the acceptable states, do nothing. Returns true if link is created.
func (backend *localFileStoreBackend) CreateLinkFromFile(fileName string, states []FileState, targetState FileState, sourcePath string) (bool, error) {
	// Verify if file exists in memory or on disk.
	_, err := backend.getFileEntry(fileName, []FileState{targetState})
	if !os.IsNotExist(err) {
		return false, err // including nil
	}

	// Create new file entry.
	entry := NewLocalFileEntry(fileName, targetState)

	mapLoadOrStore := func() (FileEntry, error) {
		v, loaded := backend.fileMap.LoadOrStore(fileName, entry)
		if loaded {
			loadedEntry := v.(FileEntry)
			loadedState, err := loadedEntry.GetState(nil) // It's another object, so it's ok to call functions that calls Lock()
			if err != nil {
				return nil, err
			}
			for _, state := range states {
				if loadedState == state {
					return loadedEntry, nil // File already exists in one of acceptable states.
				}
			}
			return nil, &FileStateError{Op: "create", State: loadedState, Name: fileName, Msg: fmt.Sprintf("Desired states: %v", states)}
		}

		return nil, nil
	}

	// Create link on disk.
	return entry.CreateLinkFrom(nil, targetState, sourcePath, mapLoadOrStore)
}

// LinkToFile create a hardlink from a file in file store to unmanaged location.
func (backend *localFileStoreBackend) LinkToFile(fileName string, states []FileState, targetPath string) error {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return err
	}

	return entry.LinkTo(states, targetPath)
}

// MoveFile moves a file to a different directory and updates its state accordingly.
func (backend *localFileStoreBackend) MoveFile(fileName string, states []FileState, targetState FileState) error {
	// Verify if file already exists in target state.
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return err
	}

	return entry.Move(states, targetState)
}

// DeleteFile removes a file from disk and file map.
func (backend *localFileStoreBackend) DeleteFile(fileName string, states []FileState) error {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return err
	}

	mapDelete := func() (FileEntry, error) {
		backend.fileMap.Delete(entry)
		return nil, nil
	}

	return entry.Delete(states, mapDelete)
}

// GetFilePath returns full path for a file.
func (backend *localFileStoreBackend) GetFilePath(fileName string, states []FileState) (string, error) {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return "", err
	}
	return entry.GetPath(states)
}

// GetFileStat returns FileInfo for a file.
func (backend *localFileStoreBackend) GetFileStat(fileName string, states []FileState) (os.FileInfo, error) {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}
	return entry.Stat(states)
}

// ReadFileMetadata returns metadata assocciate with the file
func (backend *localFileStoreBackend) ReadFileMetadata(fileName string, states []FileState, mt MetadataType) ([]byte, error) {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}
	return entry.ReadMetadata(states, mt)
}

// WriteFileMetadata creates or overwrites metadata assocciate with the file with content
func (backend *localFileStoreBackend) WriteFileMetadata(fileName string, states []FileState, mt MetadataType, data []byte) (bool, error) {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return false, err
	}
	return entry.WriteMetadata(states, mt, data)
}

// ReadFileMetadataAt returns metadata assocciate with the file
func (backend *localFileStoreBackend) ReadFileMetadataAt(fileName string, states []FileState, mt MetadataType, b []byte, off int64) (int, error) {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	return entry.ReadMetadataAt(states, mt, b, off)
}

// WriteFileMetadataAt overwrites metadata assocciate with the file with content.
func (backend *localFileStoreBackend) WriteFileMetadataAt(fileName string, states []FileState, mt MetadataType, b []byte, off int64) (int, error) {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	return entry.WriteMetadataAt(states, mt, b, off)
}

// DeleteFileMetadata deletes metadata of the specified type for a file.
func (backend *localFileStoreBackend) DeleteFileMetadata(fileName string, states []FileState, mt MetadataType) error {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return err
	}
	return entry.DeleteMetadata(states, mt)
}

// ListFileMetadata returns a list of all metadata for a file.
func (backend *localFileStoreBackend) ListFileMetadata(fileName string, states []FileState) ([]MetadataType, error) {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}

	return entry.ListMetadata(states)
}

// GetFileReader returns a FileReader object for read operations.
func (backend *localFileStoreBackend) GetFileReader(fileName string, states []FileState) (FileReader, error) {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}

	return entry.GetReader(states)
}

// GetFileReadWriter returns a FileReadWriter object for read/write operations.
func (backend *localFileStoreBackend) GetFileReadWriter(fileName string, states []FileState) (FileReadWriter, error) {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}

	return entry.GetReadWriter(states)
}

// IncrementFileRefCount increments file ref count. Ref count is stored in a metadata file on local disk.
func (backend *localFileStoreBackend) IncrementFileRefCount(fileName string, states []FileState) (int64, error) {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	return entry.IncrementRefCount(states)
}

// DecrementFileRefCount decrements file ref count. Ref count is stored in a metadata file on local disk.
func (backend *localFileStoreBackend) DecrementFileRefCount(fileName string, states []FileState) (int64, error) {
	entry, err := backend.getFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	return entry.DecrementRefCount(states)
}
