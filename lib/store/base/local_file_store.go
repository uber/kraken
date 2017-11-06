package base

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
)

// LocalFileStoreBuilder initialized FileStore objects.
type LocalFileStoreBuilder struct {
	fileEntryInternalFactory FileEntryInternalFactory
	fileEntryFactory         FileEntryFactory
	fileMapFactory           FileMapFactory
}

// Build initializes and returns a new FileStore object.
func (f *LocalFileStoreBuilder) Build() (FileStore, error) {
	if f.fileEntryInternalFactory == nil {
		f.fileEntryInternalFactory = &LocalFileEntryInternalFactory{}
	}
	if f.fileEntryFactory == nil {
		f.fileEntryFactory = &LocalFileEntryFactory{}
	}
	if f.fileMapFactory == nil {
		f.fileMapFactory = &DefaultFileMapFactory{}
	}

	return NewLocalFileStore(f.fileEntryInternalFactory, f.fileEntryFactory, f.fileMapFactory)
}

// SetFileEntryInternalFactory sets the factory used to init FileEntryInternal.
func (f *LocalFileStoreBuilder) SetFileEntryInternalFactory(fileEntryInternalFactory FileEntryInternalFactory) FileStoreBuilder {
	f.fileEntryInternalFactory = fileEntryInternalFactory
	return f
}

// SetFileEntryFactory sets the factory used to init FileEntry.
func (f *LocalFileStoreBuilder) SetFileEntryFactory(fileEntryFactory FileEntryFactory) FileStoreBuilder {
	f.fileEntryFactory = fileEntryFactory
	return f
}

// SetFileMapFactory sets the factory used to init FileMap.
func (f *LocalFileStoreBuilder) SetFileMapFactory(fileMapFactory FileMapFactory) FileStoreBuilder {
	f.fileMapFactory = fileMapFactory
	return f
}

// LocalFileStore manages all agent files on local disk.
// Read/Write operation should access data in this order:
//   map load -> file lock -> verify not deleted -> map load/store -> file/metadata change -> file unlock
// Delete opereration should access data in this order:
//   map load -> file lock -> verify not deleted -> file/metadata change -> delete from map -> file unlock
type LocalFileStore struct {
	fileEntryInternalFactory FileEntryInternalFactory // Used for dependency injection.
	fileEntryFactory         FileEntryFactory         // Used for dependency injection.
	fileMap                  FileMap
}

// NewLocalFileStore initializes and returns a new FileStore object. It allows dependency injection.
// TODO (@evelynl): maybe we should refactor this...
func NewLocalFileStore(
	fileEntryInternalFactory FileEntryInternalFactory,
	fileEntryFactory FileEntryFactory,
	fileMapFactory FileMapFactory) (*LocalFileStore, error) {
	fileMap, err := fileMapFactory.Create()
	if err != nil {
		return nil, err
	}
	return &LocalFileStore{
		fileEntryInternalFactory: fileEntryInternalFactory,
		fileEntryFactory:         fileEntryFactory,
		fileMap:                  fileMap,
	}, nil
}

func (s *LocalFileStore) createFileEntry(fileName string, state FileState) FileEntry {
	fi := s.fileEntryInternalFactory.Create(state.GetDirectory(), fileName)
	fileEntry := s.fileEntryFactory.Create(state, fi)
	return fileEntry
}

// LoadFileEntry is a helper function that returns file entry from map and a verification helper
// function.
// If the file is not in map, it tries to reload from disk.
// TODO: Hide this function?
func (s *LocalFileStore) LoadFileEntry(fileName string, states []FileState) (FileEntry, Verify, error) {
	var fileEntry FileEntry
	entry, exists := s.fileMap.Load(fileName)
	if exists {
		fileEntry = entry.(FileEntry)
	} else {
		// Check if file exists on disk.
		for _, state := range states {
			fp := path.Join(state.GetDirectory(), s.fileEntryInternalFactory.GetRelativePath(fileName))
			if _, err := os.Stat(fp); err != nil {
				// File doesn't exists on disk.
				continue
			}
			fileEntry = s.createFileEntry(fileName, state)

			// Load metadata.
			paths, err := filepath.Glob(fp + "*")
			if err != nil {
				continue
			}
			for _, path := range paths {
				// Glob could return the data file itself, and directories. Verify it's actually a metadata file.
				suffix := path[len(fp):]
				mt := CreateFromSuffix(suffix)
				if mt != nil {
					// Add metadata
					fileEntry.AddMetadata(func(FileEntry) error { return nil }, mt)
				}
			}

			if err == nil {
				// Try to store file entry into memory.
				// It's possible the entry exists now, in that case just use existing obj.
				entry, _ := s.fileMap.LoadOrStore(fileName, fileEntry)
				fileEntry = entry.(FileEntry)
				exists = true
			}
			break
		}
	}

	if !exists {
		return nil, nil, &os.PathError{Op: "get", Path: fileName, Err: os.ErrNotExist}
	}

	// Construct verification function.
	v := func(entry FileEntry) error {
		// Verify entry hasn't been deleted.
		if _, loaded := s.fileMap.Load(fileName); !loaded {
			return &os.PathError{Op: "get", Path: fileName, Err: os.ErrNotExist}
		}

		// Skips state check if states is nil.
		if states == nil {
			return nil
		}

		// Verify state.
		for _, state := range states {
			if entry.GetStateUnsafe() == state {
				return nil
			}
		}
		return &FileStateError{
			Op:    "get",
			State: entry.GetStateUnsafe(),
			Name:  fileName,
			Msg:   fmt.Sprintf("Desired states: %v", states),
		}
	}

	return fileEntry, v, nil
}

// CreateFile creates an empty file with specified size.
// If file exists and is in one of the acceptable states, returns os.ErrExist.
// If file exists but not in one of the acceptable states, returns FileStateError.
func (s *LocalFileStore) CreateFile(fileName string, states []FileState, targetState FileState, len int64) error {
	// Verify if file exists in memory or on disk.
	_, _, err := s.LoadFileEntry(fileName, states)
	if err == nil {
		// File already exists in one of acceptable states.
		return os.ErrExist
	} else if !os.IsNotExist(err) {
		// Includes FileStateError.
		return err
	}

	// Create new file entry.
	newEntry := s.createFileEntry(fileName, targetState)

	// Instead of verification, try store the new entry into map.
	mapLoadOrStore := func(FileEntry) error { // Cannot use parameter because it will be of static type LocalFileEntry
		value, loaded := s.fileMap.LoadOrStore(fileName, newEntry)
		if loaded {
			// It's another object, so it's ok to call functions that calls Lock()
			loadedState, err := value.(FileEntry).GetState(noopVerify)
			if err != nil {
				return err
			}
			for _, state := range states {
				if loadedState == state {
					return os.ErrExist // File already exists in one of acceptable states.
				}
			}
			return &FileStateError{
				Op:    "create",
				State: loadedState,
				Name:  fileName,
				Msg:   fmt.Sprintf("Desired states: %v", states),
			}
		}
		return nil
	}

	// Create file on disk.
	return newEntry.Create(mapLoadOrStore, targetState, len)
}

// CreateLinkFromFile create a hardlink of a file from unmanaged location to file store.
// If file exists and is in one of the acceptable states, returns os.ErrExist.
// If file exists but not in one of the acceptable states, returns FileStateError.
func (s *LocalFileStore) CreateLinkFromFile(fileName string, states []FileState, targetState FileState, sourcePath string) error {
	// Verify if file exists in memory or on disk.
	_, _, err := s.LoadFileEntry(fileName, []FileState{targetState})
	if err == nil {
		// File already exists in one of acceptable states.
		return os.ErrExist
	} else if !os.IsNotExist(err) {
		// Includes FileStateError.
		return err
	}

	// Create new file entry.
	newEntry := s.createFileEntry(fileName, targetState)

	// Instead of verification, try store the new entry into map.
	mapLoadOrStore := func(FileEntry) error { // Cannot use parameter because it will be of static type LocalFileEntry
		value, loaded := s.fileMap.LoadOrStore(fileName, newEntry)
		if loaded {
			// It's another object, so it's ok to call functions that calls Lock()
			loadedState, err := value.(FileEntry).GetState(noopVerify)
			if err != nil {
				return err
			}
			for _, state := range states {
				if loadedState == state {
					// File already exists in one of acceptable states.
					return os.ErrExist
				}
			}
			return &FileStateError{
				Op:    "create",
				State: loadedState,
				Name:  fileName,
				Msg:   fmt.Sprintf("Desired states: %v", states),
			}
		}
		return nil
	}

	// Create link on disk.
	return newEntry.CreateLinkFrom(mapLoadOrStore, targetState, sourcePath)
}

// LinkToFile create a hardlink from a file in file store to unmanaged location.
func (s *LocalFileStore) LinkToFile(fileName string, states []FileState, targetPath string) error {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return err
	}

	return entry.LinkTo(v, targetPath)
}

// MoveFile moves a file to a different directory and updates its state accordingly.
func (s *LocalFileStore) MoveFile(fileName string, states []FileState, targetState FileState) error {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return err
	}

	moreVerify := func(entry FileEntry) error {
		if entry.GetStateUnsafe() == targetState {
			return os.ErrExist
		}
		return v(entry)
	}

	return entry.Move(moreVerify, targetState)
}

// DeleteFile removes a file from disk and file map.
func (s *LocalFileStore) DeleteFile(fileName string, states []FileState) error {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return err
	}

	mapDelete := func(entry FileEntry) error {
		defer s.fileMap.Delete(entry)
		return v(entry)
	}

	return entry.Delete(mapDelete)
}

// GetFilePath returns full path for a file.
func (s *LocalFileStore) GetFilePath(fileName string, states []FileState) (string, error) {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return "", err
	}
	return entry.GetPath(v)
}

// GetFileStat returns FileInfo for a file.
func (s *LocalFileStore) GetFileStat(fileName string, states []FileState) (os.FileInfo, error) {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}
	return entry.Stat(v)
}

// GetFileReader returns a FileReader object for read operations.
func (s *LocalFileStore) GetFileReader(fileName string, states []FileState) (FileReader, error) {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}

	return entry.GetReader(v)
}

// GetFileReadWriter returns a FileReadWriter object for read/write operations.
func (s *LocalFileStore) GetFileReadWriter(fileName string, states []FileState) (FileReadWriter, error) {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}

	return entry.GetReadWriter(v)
}

// ReadFileMetadata returns metadata assocciated with the file
func (s *LocalFileStore) ReadFileMetadata(fileName string, states []FileState, mt MetadataType) ([]byte, error) {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return nil, err
	}
	return entry.ReadMetadata(v, mt)
}

// WriteFileMetadata creates or overwrites metadata assocciate with the file with content
func (s *LocalFileStore) WriteFileMetadata(fileName string, states []FileState, mt MetadataType, data []byte) (bool, error) {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return false, err
	}
	return entry.WriteMetadata(v, mt, data)
}

// ReadFileMetadataAt returns metadata assocciate with the file
func (s *LocalFileStore) ReadFileMetadataAt(fileName string, states []FileState, mt MetadataType, b []byte, off int64) (int, error) {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	return entry.ReadMetadataAt(v, mt, b, off)
}

// WriteFileMetadataAt overwrites metadata assocciate with the file with content.
func (s *LocalFileStore) WriteFileMetadataAt(fileName string, states []FileState, mt MetadataType, b []byte, off int64) (int, error) {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return 0, err
	}

	return entry.WriteMetadataAt(v, mt, b, off)
}

// DeleteFileMetadata deletes metadata of the specified type for a file.
func (s *LocalFileStore) DeleteFileMetadata(fileName string, states []FileState, mt MetadataType) error {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return err
	}
	return entry.DeleteMetadata(v, mt)
}

// RangeFileMetadata loops through all metadata of one file and applies function f, until an error happens.
func (s *LocalFileStore) RangeFileMetadata(fileName string, states []FileState, f func(mt MetadataType) error) error {
	entry, v, err := s.LoadFileEntry(fileName, states)
	if err != nil {
		return err
	}
	return entry.RangeMetadata(v, f)
}
