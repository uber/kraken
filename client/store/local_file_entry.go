package store

import (
	"os"
	"path"
	"sync"
)

// FileEntry keeps basic information of a file, and provides reader and readwriter on the file.
type FileEntry interface {
	GetName() string
	GetPath() string
	GetState() FileState
	SetState(state FileState)
	IsOpen() bool
	Stat() (os.FileInfo, error)
	GetFileReader() (FileReader, error)
	GetFileReadWriter() (FileReadWriter, error)
	SetMetadata(metadataType, []byte) (bool, error)
	GetMetadata(metadataType) ([]byte, error)
}

// LocalFileEntry keeps information of a file on local disk.
type localFileEntry struct {
	sync.RWMutex // Though most operations happen under a global lock, we still need a lock here for Close()

	name      string
	state     FileState
	openCount int
}

// NewLocalFileEntry initializes and returns a new LocalFileEntryLocalFileEntry object.
func NewLocalFileEntry(name string, state FileState) FileEntry {
	return &localFileEntry{
		name:      name,
		state:     state,
		openCount: 0,
	}
}

func (entry *localFileEntry) GetName() string {
	return entry.name
}

func (entry *localFileEntry) GetPath() string {
	return path.Join(entry.state.GetDirectory(), entry.name)
}

func (entry *localFileEntry) GetState() FileState {
	return entry.state
}

func (entry *localFileEntry) SetState(state FileState) {
	entry.Lock()
	defer entry.Unlock()

	entry.state = state
}

// SetMetadatacreates metadata file for the file, if file exists, overwrites
func (entry *localFileEntry) SetMetadata(mt metadataType, content []byte) (bool, error) {
	entry.Lock()
	defer entry.Unlock()

	filePath := path.Join(entry.state.GetDirectory(), entry.name)
	return mt.set(filePath, content)
}

// GetMetadata returns metadata for the file
func (entry *localFileEntry) GetMetadata(mt metadataType) ([]byte, error) {
	entry.Lock()
	defer entry.Unlock()

	filePath := path.Join(entry.state.GetDirectory(), entry.name)
	return mt.get(filePath)
}

// DeleteMetadata deletes metadata file for the file, if file doesnt exit, do nothing
func (entry *localFileEntry) DeleteMetadata(mt metadataType) error {
	entry.Lock()
	defer entry.Unlock()

	filePath := path.Join(entry.state.GetDirectory(), entry.name)
	return mt.delete(filePath)
}

// IsOpen check if any caller still has this file open.
func (entry *localFileEntry) IsOpen() bool {
	entry.RLock()
	defer entry.RUnlock()

	return entry.openCount > 0
}

// Stat returns a FileInfo describing the named file
func (entry *localFileEntry) Stat() (os.FileInfo, error) {
	entry.RLock()
	defer entry.RUnlock()

	f, err := os.OpenFile(path.Join(entry.state.GetDirectory(), entry.name), os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}

	return f.Stat()
}

// GetFileReader returns a FileReader object for read operations.
func (entry *localFileEntry) GetFileReader() (FileReader, error) {
	entry.RLock()
	defer entry.RUnlock()

	f, err := os.OpenFile(path.Join(entry.state.GetDirectory(), entry.name), os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	entry.openCount++

	reader := &localFileReadWriter{
		entry:      entry,
		descriptor: f,
	}
	return reader, nil
}

// GetReadWriter returns a FileReadWriter object for read/write operations.
func (entry *localFileEntry) GetFileReadWriter() (FileReadWriter, error) {
	entry.RLock()
	defer entry.RUnlock()

	f, err := os.OpenFile(path.Join(entry.state.GetDirectory(), entry.name), os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	entry.openCount++

	readWriter := &localFileReadWriter{
		entry:      entry,
		descriptor: f,
	}
	return readWriter, nil
}
