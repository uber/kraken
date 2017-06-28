package base

import (
	"os"
	"sync"
)

// LocalFileEntryFactory is responsible for initializing LocalFileEntry objects.
type LocalFileEntryFactory struct {
}

// Create initializes and returns a FileEntry object.
func (f *LocalFileEntryFactory) Create(state FileState, fi FileEntryInternal) FileEntry {
	return &LocalFileEntry{
		state: state,
		fi:    fi,
	}
}

// LocalFileEntry implements FileEntry interface, manages one file and its metadata on local disk.
type LocalFileEntry struct {
	sync.RWMutex

	state FileState
	fi    FileEntryInternal
}

// GetInternal returns FileEntryInternal object.
func (entry *LocalFileEntry) GetInternal() FileEntryInternal {
	return entry.fi
}

// GetName returns name of the file.
func (entry *LocalFileEntry) GetName(v Verify) (string, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := v(entry); err != nil {
		return "", err
	}

	return entry.fi.GetName(), nil
}

// GetPath returns current path of the file.
func (entry *LocalFileEntry) GetPath(v Verify) (string, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := v(entry); err != nil {
		return "", err
	}

	return entry.fi.GetPath(), nil
}

// GetStateUnsafe returns current state of the file without lock.
func (entry *LocalFileEntry) GetStateUnsafe() FileState {
	return entry.state
}

// GetState returns current state of the file.
func (entry *LocalFileEntry) GetState(v Verify) (FileState, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := v(entry); err != nil {
		return nil, err
	}

	return entry.state, nil
}

// SetState sets current state of the file.
func (entry *LocalFileEntry) SetState(v Verify, state FileState) error {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil {
		return err
	}

	entry.state = state
	return nil
}

// Stat returns a FileInfo describing the named file.
func (entry *LocalFileEntry) Stat(v Verify) (os.FileInfo, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := v(entry); err != nil {
		return nil, err
	}

	return entry.fi.Stat()
}

// Create creates a file on disk.
func (entry *LocalFileEntry) Create(v Verify, targetState FileState, len int64) error {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil { // Insert into map
		return err
	}

	// Create file
	return entry.fi.Create(len)
}

// CreateLinkFrom creates a hardlink from an unmanaged file.
func (entry *LocalFileEntry) CreateLinkFrom(v Verify, targetState FileState, sourcePath string) error {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil { // Insert into map
		return err
	}

	// Create hardlink
	return entry.fi.CreateLinkFrom(sourcePath)
}

// LinkTo creates a hardlink to an unmanaged file.
func (entry *LocalFileEntry) LinkTo(v Verify, targetPath string) error {
	entry.RLock()
	defer entry.RUnlock()
	if err := v(entry); err != nil {
		return err
	}

	// Create hardlink
	return entry.fi.LinkTo(targetPath)
}

// Move moves file to another directory and upload state accordingly.
func (entry *LocalFileEntry) Move(v Verify, targetState FileState) error {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil {
		return err
	}

	err := entry.fi.Move(targetState.GetDirectory())
	if err == nil {
		entry.state = targetState
	}
	return err
}

// Delete removes file from disk.
func (entry *LocalFileEntry) Delete(v Verify) error {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil { // Delete from map.
		return err
	}
	return entry.fi.Delete()
}

// GetReader returns a FileReader object for read fierations.
func (entry *LocalFileEntry) GetReader(v Verify) (FileReader, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := v(entry); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(entry.fi.GetPath(), os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}

	reader := &localFileReadWriter{
		entry:      entry,
		descriptor: f,
	}
	return reader, nil
}

// GetReadWriter returns a FileReadWriter object for read/write fierations.
func (entry *LocalFileEntry) GetReadWriter(v Verify) (FileReadWriter, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := v(entry); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(entry.fi.GetPath(), os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}

	readWriter := &localFileReadWriter{
		entry:      entry,
		descriptor: f,
	}
	return readWriter, nil
}

// AddMetadata adds a new metadata type to metadataSet. This is primirily used during reload.
func (entry *LocalFileEntry) AddMetadata(v Verify, mt MetadataType) error {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil {
		return err
	}
	return entry.fi.AddMetadata(mt)
}

// ReadMetadata returns metadata content as a byte array.
func (entry *LocalFileEntry) ReadMetadata(v Verify, mt MetadataType) ([]byte, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := v(entry); err != nil {
		return nil, err
	}
	return entry.fi.ReadMetadata(mt)
}

// WriteMetadata updates metadata and returns true only if the file is updated correctly;
// Returns false if error happened or file already contains desired content.
func (entry *LocalFileEntry) WriteMetadata(v Verify, mt MetadataType, b []byte) (bool, error) {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil {
		return false, err
	}
	return entry.fi.WriteMetadata(mt, b)
}

// ReadMetadataAt reads metadata at specified offset.
func (entry *LocalFileEntry) ReadMetadataAt(v Verify, mt MetadataType, b []byte, off int64) (int, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := v(entry); err != nil {
		return 0, err
	}
	return entry.fi.ReadMetadataAt(mt, b, off)
}

// WriteMetadataAt writes metadata at specified offset.
func (entry *LocalFileEntry) WriteMetadataAt(v Verify, mt MetadataType, b []byte, off int64) (int, error) {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil {
		return 0, err
	}
	return entry.fi.WriteMetadataAt(mt, b, off)
}

// DeleteMetadata deletes metadata of the specified type.
func (entry *LocalFileEntry) DeleteMetadata(v Verify, mt MetadataType) error {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil {
		return err
	}
	return entry.fi.DeleteMetadata(mt)
}

// RangeMetadata lofis through all metadata and applies function f, until an error happens.
func (entry *LocalFileEntry) RangeMetadata(v Verify, f func(mt MetadataType) error) error {
	entry.Lock()
	defer entry.Unlock()
	if err := v(entry); err != nil {
		return err
	}
	return entry.fi.RangeMetadata(f)
}
