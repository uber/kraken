package store

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"code.uber.internal/infra/kraken/utils"
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

	ReadMetadata(mt MetadataType) ([]byte, error)
	WriteMetadata(mt MetadataType, data []byte) (bool, error)
	ReadMetadataAt(mt MetadataType, b []byte, off int64) (int, error)
	WriteMetadataAt(mt MetadataType, b []byte, off int64) (int, error)
	DeleteMetadata(mt MetadataType) error
	ListMetadata() []MetadataType

	GetRefCount() (int64, error)
	IncrementRefCount() (int64, error)
	DecrementRefCount() (int64, error)
}

// localFileEntry keeps information of a file on local disk.
type localFileEntry struct {
	sync.RWMutex // Though most operations happen under a global lock, we still need a lock here for Close()

	name        string
	state       FileState
	openCount   int
	metadataMap map[MetadataType]bool
}

// NewLocalFileEntry initializes and returns a LocalFileEntry object.
func NewLocalFileEntry(name string, state FileState) FileEntry {
	return &localFileEntry{
		name:        name,
		state:       state,
		openCount:   0,
		metadataMap: make(map[MetadataType]bool),
	}
}

// GetName returns name of the file.
func (entry *localFileEntry) GetName() string {
	entry.RLock()
	defer entry.RUnlock()

	return entry.name
}

// GetPath returns current path of the file.
func (entry *localFileEntry) GetPath() string {
	entry.RLock()
	defer entry.RUnlock()

	return path.Join(entry.state.GetDirectory(), entry.name)
}

// GetState returns current state of the file.
func (entry *localFileEntry) GetState() FileState {
	entry.RLock()
	defer entry.RUnlock()

	return entry.state
}

// SetState sets current state of the file.
func (entry *localFileEntry) SetState(state FileState) {
	entry.Lock()
	defer entry.Unlock()

	entry.state = state
}

// IsOpen check if any caller still has this file open.
func (entry *localFileEntry) IsOpen() bool {
	entry.RLock()
	defer entry.RUnlock()

	return entry.openCount > 0
}

// Stat returns a FileInfo describing the named file.
func (entry *localFileEntry) Stat() (os.FileInfo, error) {
	entry.RLock()
	defer entry.RUnlock()

	return os.Stat(path.Join(entry.state.GetDirectory(), entry.name))
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

// GetFileReadWriter returns a FileReadWriter object for read/write operations.
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

// ReadMetadata returns metadata content as a byte array.
func (entry *localFileEntry) ReadMetadata(mt MetadataType) ([]byte, error) {
	entry.RLock()
	defer entry.RUnlock()

	return entry.readMetadata(mt)
}

func (entry *localFileEntry) readMetadata(mt MetadataType) ([]byte, error) {
	fp := path.Join(entry.state.GetDirectory(), entry.name+mt.Suffix())

	// Check existence.
	if _, err := os.Stat(fp); err != nil {
		return nil, err
	}

	// In case this is during reload.
	entry.metadataMap[mt] = true

	return ioutil.ReadFile(fp)
}

// WriteMetadata updates metadata and returns true only if the file is updated correctly;
// Returns false if error or file already contains desired content.
func (entry *localFileEntry) WriteMetadata(mt MetadataType, b []byte) (bool, error) {
	entry.Lock()
	defer entry.Unlock()

	return entry.writeMetadata(mt, b)
}

func (entry *localFileEntry) writeMetadata(mt MetadataType, b []byte) (bool, error) {
	fp := path.Join(entry.state.GetDirectory(), entry.name+mt.Suffix())

	// Check existence.
	fs, err := os.Stat(fp)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}

	if os.IsNotExist(err) {
		err = os.MkdirAll(path.Dir(fp), 0755)
		if err != nil {
			return false, err
		}

		err = ioutil.WriteFile(fp, b, 0755)
		if err != nil {
			return false, err
		}
		entry.metadataMap[mt] = true
		return true, nil
	}

	f, err := os.OpenFile(fp, os.O_RDWR, 0755)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Compare with existing data, overwrite if different.
	buf := make([]byte, int(fs.Size()))
	_, err = f.Read(buf)
	if err != nil {
		return false, err
	}
	if utils.CompareByteArray(buf, b) {
		return false, nil
	}

	if len(buf) != len(b) {
		err = f.Truncate(int64(len(b)))
		if err != nil {
			return false, err
		}
	}

	_, err = f.WriteAt(b, 0)
	if err != nil {
		return false, err
	}
	entry.metadataMap[mt] = true
	return true, nil
}

// ReadMetadataAt reads metadata at specified offset.
func (entry *localFileEntry) ReadMetadataAt(mt MetadataType, b []byte, off int64) (int, error) {
	entry.RLock()
	defer entry.RUnlock()

	fp := path.Join(entry.state.GetDirectory(), entry.name+mt.Suffix())

	// Check existence.
	if _, err := os.Stat(fp); err != nil {
		return 0, err
	}

	// Read to data.
	f, err := os.Open(fp)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return f.ReadAt(b, off)
}

// WriteMetadataAt writes metadata at specified offset.
func (entry *localFileEntry) WriteMetadataAt(mt MetadataType, b []byte, off int64) (int, error) {
	entry.Lock()
	defer entry.Unlock()

	fp := path.Join(entry.state.GetDirectory(), entry.name+mt.Suffix())

	// Check existence.
	_, err := os.Stat(fp)
	if err != nil {
		return 0, err
	}

	// Compare with existing data, overwrite if different.
	f, err := os.OpenFile(fp, os.O_RDWR, 0755)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	buf := make([]byte, len(b))
	_, err = f.ReadAt(buf, off)
	if err != nil {
		return 0, err
	}
	if utils.CompareByteArray(buf, b) {
		return 0, nil
	}

	return f.WriteAt(b, off)
}

// DeleteMetadata deletes metadata of the specified type.
func (entry *localFileEntry) DeleteMetadata(mt MetadataType) error {
	entry.Lock()
	defer entry.Unlock()

	fp := path.Join(entry.state.GetDirectory(), entry.name+mt.Suffix())

	err := os.RemoveAll(fp)
	if err != nil {
		return err
	}

	delete(entry.metadataMap, mt)

	return nil
}

// ListMetadata returns a list of metadata for this file.
func (entry *localFileEntry) ListMetadata() []MetadataType {
	entry.RLock()
	defer entry.RUnlock()

	var keys []MetadataType
	for k := range entry.metadataMap {
		keys = append(keys, k)
	}
	return keys
}

// GetRefCount returns current ref count. No ref count file means ref count is 0.
func (entry *localFileEntry) GetRefCount() (int64, error) {
	entry.RLock()
	defer entry.RUnlock()

	return entry.getRefCount()
}

// IncrementRefCount increments ref count by 1.
func (entry *localFileEntry) IncrementRefCount() (int64, error) {
	entry.Lock()
	defer entry.Unlock()

	return entry.updateRefCount(true)
}

// DecrementRefCount decrements ref count by 1.
func (entry *localFileEntry) DecrementRefCount() (int64, error) {
	entry.Lock()
	defer entry.Unlock()

	return entry.updateRefCount(false)
}

func (entry *localFileEntry) getRefCount() (int64, error) {
	var refCount int64
	var n int

	b, err := entry.readMetadata(getRefCount())
	if err != nil && !os.IsNotExist(err) {
		return 0, err
	} else if err == nil {
		refCount, n = binary.Varint(b)
		if n <= 0 {
			return 0, fmt.Errorf("Failed to parse ref count: %v", b)
		}
	}
	return refCount, nil
}

func (entry *localFileEntry) updateRefCount(increment bool) (int64, error) {
	refCount, err := entry.getRefCount()
	if err != nil {
		return 0, err
	}

	if increment {
		refCount++
	} else if refCount > 0 {
		refCount--
	}
	buf := make([]byte, 8)
	n := binary.PutVarint(buf, refCount)
	if n <= 0 {
		return 0, fmt.Errorf("Failed to put ref count: %d", refCount)
	}
	_, err = entry.writeMetadata(getRefCount(), buf)
	if err != nil {
		return 0, err
	}

	return refCount, nil
}
