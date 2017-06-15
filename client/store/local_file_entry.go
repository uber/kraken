package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"path/filepath"
)

// localFileEntry manages one file and its metadata on local disk, and keeps its ref count on local disk too.
type localFileEntry struct {
	localFileEntryBasic
}

// NewLocalFileEntry initializes and returns a FileEntry object.
func NewLocalFileEntry(fileName string, state FileState) FileEntry {
	return &localFileEntry{
		localFileEntryBasic{
			name:        fileName,
			state:       state,
			openCount:   0,
			metadataMap: make(map[MetadataType]struct{}),
			deleted:     false,
		},
	}
}

// ReloadLocalFileEntry tries to reload file entry from disk.
func ReloadLocalFileEntry(fileName string, state FileState) (FileEntry, error) {
	fp := path.Join(state.GetDirectory(), fileName)
	if _, err := os.Stat(fp); err != nil {
		// File doesn't exists on disk
		return nil, err
	}
	entry := NewLocalFileEntry(fileName, state)
	// Load metadata
	paths, err := filepath.Glob(fp + "*")
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		// Glob could return the data file itself, and directories. Verify it's actually a metadata file.
		mt := getMetadataType(path)
		if mt != nil {
			entry.AddMetadata(nil, mt)
		}
	}
	return entry, nil
}

// GetRefCount returns current ref count. No ref count file means ref count is 0.
func (entry *localFileEntry) GetRefCount(states []FileState) (int64, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := entry.verifyState(states); err != nil {
		return 0, err
	}

	return entry.getRefCount()
}

// IncrementRefCount increments ref count by 1.
func (entry *localFileEntry) IncrementRefCount(states []FileState) (int64, error) {
	entry.Lock()
	defer entry.Unlock()
	if err := entry.verifyState(states); err != nil {
		return 0, err
	}

	return entry.updateRefCount(true)
}

// DecrementRefCount decrements ref count by 1.
func (entry *localFileEntry) DecrementRefCount(states []FileState) (int64, error) {
	entry.Lock()
	defer entry.Unlock()
	if err := entry.verifyState(states); err != nil {
		return 0, err
	}

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
