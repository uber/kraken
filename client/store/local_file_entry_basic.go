package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"code.uber.internal/infra/kraken/utils"
)

// localFileEntryBasic implements FileEntryBasic interface, manages one file and its metadata on local disk.
type localFileEntryBasic struct {
	sync.RWMutex

	name        string
	state       FileState
	openCount   int
	metadataMap map[MetadataType]struct{}
	deleted     bool
}

// VerifyState verifies that entry hasn't been deleted, and it's in one of the desired states.
// If states is nil, it skips state check
func (entry *localFileEntry) verifyState(states []FileState) error {
	// Verify entry hasn't been deleted
	if entry.deleted {
		return &os.PathError{Op: "get", Path: entry.name, Err: os.ErrNotExist}
	}

	// Skips state check if states is nil
	if states == nil {
		return nil
	}

	// Verify state
	for _, state := range states {
		if entry.state == state {
			return nil
		}
	}
	return &FileStateError{Op: "get", State: entry.state, Name: entry.name, Msg: fmt.Sprintf("Desired states: %v", states)}
}

// GetName returns name of the file.
func (entry *localFileEntry) GetName(states []FileState) (string, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := entry.verifyState(states); err != nil {
		return "", err
	}

	return entry.name, nil
}

// GetPath returns current path of the file.
func (entry *localFileEntry) GetPath(states []FileState) (string, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := entry.verifyState(states); err != nil {
		return "", err
	}

	return path.Join(entry.state.GetDirectory(), entry.name), nil
}

// GetState returns current state of the file.
func (entry *localFileEntry) GetState(states []FileState) (FileState, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := entry.verifyState(states); err != nil {
		return nil, err
	}

	return entry.state, nil
}

// SetState sets current state of the file.
func (entry *localFileEntry) SetState(states []FileState, state FileState) error {
	entry.Lock()
	defer entry.Unlock()
	if err := entry.verifyState(states); err != nil {
		return err
	}

	entry.state = state
	return nil
}

// IsOpen check if any caller still has this file open.
func (entry *localFileEntry) IsOpen(states []FileState) (bool, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := entry.verifyState(states); err != nil {
		return false, err
	}

	return entry.openCount > 0, nil
}

// Stat returns a FileInfo describing the named file.
func (entry *localFileEntry) Stat(states []FileState) (os.FileInfo, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := entry.verifyState(states); err != nil {
		return nil, err
	}

	return os.Stat(path.Join(entry.state.GetDirectory(), entry.name))
}

// CreateFile creates a file on disk.
func (entry *localFileEntry) Create(states []FileState, targetState FileState, len int64, callback BackendCallback) (bool, error) {
	entry.Lock()
	defer entry.Unlock()

	// Store file entry into file map, or load existing one.
	loadedEntry, err := callback()
	if err != nil {
		return false, err
	}
	if loadedEntry != nil {
		return false, nil
	}

	// Create file
	targetPath := path.Join(targetState.GetDirectory(), entry.name)
	f, err := os.Create(targetPath)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Change size
	err = f.Truncate(len)
	if err != nil {
		// Try to delete file
		os.Remove(targetPath)
		return false, err
	}
	return true, nil
}

// CreateLinkFrom creates a hardlink from an unmanaged file.
func (entry *localFileEntry) CreateLinkFrom(states []FileState, targetState FileState, sourcePath string, callback BackendCallback) (bool, error) {
	entry.Lock()
	defer entry.Unlock()

	// Store file entry into file map, or load existing one.
	loadedEntry, err := callback()
	if err != nil {
		return false, err
	}
	if loadedEntry != nil {
		return false, nil
	}

	// Create hardlink
	targetPath := path.Join(targetState.GetDirectory(), entry.name)
	if err := os.Link(sourcePath, targetPath); err != nil {
		return false, err
	}
	return true, nil
}

// Delete removes file from disk.
// Note: it doesn't remove file entry from map.
func (entry *localFileEntry) Delete(states []FileState, callback BackendCallback) error {
	entry.Lock()
	defer entry.Unlock()
	if err := entry.verifyState(states); err != nil {
		return err
	}

	refCount, err := entry.getRefCount()
	if err != nil {
		return err
	}
	if refCount > 0 {
		return &RefCountError{Op: "move", State: entry.state, Name: entry.name, RefCount: refCount, Msg: fmt.Sprintf("File still referenced")}
	}
	if entry.openCount > 0 {
		// TODO: it is possible that the file is still open after its moved to trash directory.
		// Read/write and os.Remove() will not be affected as it will be handled by the file system.
		// However this is not very nice if we have open files in trash dir.
	}

	// Get list of metadata.
	var sourceMetadataPaths []string
	for _, mt := range entry.listMetadata() {
		sourceMetadataPath := path.Join(entry.state.GetDirectory(), entry.name+mt.Suffix())
		sourceMetadataPaths = append(sourceMetadataPaths, sourceMetadataPath)
	}

	// Remove data file
	os.Remove(path.Join(entry.state.GetDirectory(), entry.name))

	// Remove old metadata files, ignore error.
	for _, sourceMetadataPath := range sourceMetadataPaths {
		os.RemoveAll(sourceMetadataPath)
	}

	// Mark as deleted
	entry.deleted = true

	// Delete from map
	_, err = callback()
	return err
}

// LinkTo creates a hardlink to an unmanaged file.
func (entry *localFileEntry) LinkTo(states []FileState, targetPath string) error {
	entry.Lock()
	defer entry.Unlock()
	if err := entry.verifyState(states); err != nil {
		return err
	}

	// Create hardlink
	sourcePath := path.Join(entry.state.GetDirectory(), entry.name)
	err := os.Link(sourcePath, targetPath)
	if err != nil {
		return err
	}
	return nil
}

func (entry *localFileEntry) moveHelper(targetState FileState) error {
	targetPath := path.Join(targetState.GetDirectory(), entry.name)

	refCount, err := entry.getRefCount()
	if err != nil {
		return err
	}
	if refCount > 0 {
		return &RefCountError{Op: "move", State: entry.state, Name: entry.name, RefCount: refCount, Msg: fmt.Sprintf("File still referenced")}
	}
	if entry.openCount > 0 {
		// TODO: it is possible that the file is still open after its moved to trash directory.
		// Read/write and os.Remove() will not be affected as it will be handled by the file system.
		// However this is not very nice if we have open files in trash dir.
	}

	// Copy metadata first. Use copy instead of move here, so any failure would be recoverable.
	var sourceMetadataPaths []string
	for _, mt := range entry.listMetadata() {
		b, err := entry.readMetadata(mt)
		if err != nil {
			return err
		}
		sourceMetadataPath := path.Join(entry.state.GetDirectory(), entry.name+mt.Suffix())
		sourceMetadataPaths = append(sourceMetadataPaths, sourceMetadataPath)

		if mt.IsValidState(targetState) {
			targetMetadataPath := path.Join((targetState).GetDirectory(), entry.name+mt.Suffix())

			err = os.MkdirAll(path.Dir(targetMetadataPath), 0755)
			if err != nil {
				return err
			}
			if err = ioutil.WriteFile(targetMetadataPath, b, 0755); err != nil {
				return err
			}
		}
	}

	// Move data file.
	sourcePath := path.Join(entry.state.GetDirectory(), entry.name)
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	// Remove old metadata files, ignore error.
	for _, sourceMetadataPath := range sourceMetadataPaths {
		os.RemoveAll(sourceMetadataPath)
	}
	return nil
}

// Move moves file to another directory and upload state accordingly.
func (entry *localFileEntry) Move(states []FileState, targetState FileState) error {
	entry.Lock()
	defer entry.Unlock()
	if entry.state == targetState {
		return &os.PathError{Op: "move", Path: entry.name, Err: os.ErrExist}
	}

	if err := entry.verifyState(states); err != nil {
		return err
	}

	if err := entry.moveHelper(targetState); err != nil {
		return err
	}

	entry.state = targetState
	return nil
}

// GetReader returns a FileReader object for read operations.
func (entry *localFileEntry) GetReader(states []FileState) (FileReader, error) {
	entry.Lock() // Need write lock here because openCount will be modified.
	defer entry.Unlock()
	if err := entry.verifyState(states); err != nil {
		return nil, err
	}

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
func (entry *localFileEntry) GetReadWriter(states []FileState) (FileReadWriter, error) {
	entry.Lock() // Need write lock here because openCount will be modified.
	defer entry.Unlock()
	if err := entry.verifyState(states); err != nil {
		return nil, err
	}

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

func (entry *localFileEntry) addMetadata(mt MetadataType) error {
	fp := path.Join(entry.state.GetDirectory(), entry.name+mt.Suffix())

	// Check existence.
	if _, err := os.Stat(fp); err != nil {
		return err
	}

	entry.metadataMap[mt] = struct{}{}
	return nil
}

// AddMetadata add one entry to metadata map. It doesn't verify if metadata file actually exists.
func (entry *localFileEntry) AddMetadata(states []FileState, mt MetadataType) error {
	entry.Lock()
	defer entry.Unlock()
	if err := entry.verifyState(states); err != nil {
		return err
	}

	return entry.addMetadata(mt)
}

// ReadMetadata returns metadata content as a byte array.
func (entry *localFileEntry) ReadMetadata(states []FileState, mt MetadataType) ([]byte, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := entry.verifyState(states); err != nil {
		return nil, err
	}

	return entry.readMetadata(mt)
}

func (entry *localFileEntry) readMetadata(mt MetadataType) ([]byte, error) {
	fp := path.Join(entry.state.GetDirectory(), entry.name+mt.Suffix())

	// Check existence.
	if _, err := os.Stat(fp); err != nil {
		return nil, err
	}

	// In case this is during reload.
	entry.metadataMap[mt] = struct{}{}

	return ioutil.ReadFile(fp)
}

// WriteMetadata updates metadata and returns true only if the file is updated correctly;
// Returns false if error or file already contains desired content.
func (entry *localFileEntry) WriteMetadata(states []FileState, mt MetadataType, b []byte) (bool, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := entry.verifyState(states); err != nil {
		return false, err
	}

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
		entry.metadataMap[mt] = struct{}{}
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
	entry.metadataMap[mt] = struct{}{}
	return true, nil
}

// ReadMetadataAt reads metadata at specified offset.
func (entry *localFileEntry) ReadMetadataAt(states []FileState, mt MetadataType, b []byte, off int64) (int, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := entry.verifyState(states); err != nil {
		return 0, err
	}

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
func (entry *localFileEntry) WriteMetadataAt(states []FileState, mt MetadataType, b []byte, off int64) (int, error) {
	entry.Lock()
	defer entry.Unlock()
	if err := entry.verifyState(states); err != nil {
		return 0, err
	}

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
func (entry *localFileEntry) DeleteMetadata(states []FileState, mt MetadataType) error {
	entry.Lock()
	defer entry.Unlock()
	if err := entry.verifyState(states); err != nil {
		return err
	}

	fp := path.Join(entry.state.GetDirectory(), entry.name+mt.Suffix())

	err := os.RemoveAll(fp)
	if err != nil {
		return err
	}

	delete(entry.metadataMap, mt)

	return nil
}

// listMetadata returns a list of metadata for this file without lock.
func (entry *localFileEntry) listMetadata() []MetadataType {
	var keys []MetadataType
	for k := range entry.metadataMap {
		keys = append(keys, k)
	}
	return keys
}

// ListMetadata returns a list of metadata for this file.
func (entry *localFileEntry) ListMetadata(states []FileState) ([]MetadataType, error) {
	entry.RLock()
	defer entry.RUnlock()
	if err := entry.verifyState(states); err != nil {
		return nil, err
	}

	return entry.listMetadata(), nil
}
