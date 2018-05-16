package base

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

// FileState decides what directory a file is in.
// A file can only be in one state at any given time.
type FileState interface {
	GetDirectory() string
}

// FileEntryFactory initializes FileEntry obj.
type FileEntryFactory interface {
	// Create creates a file entry given a state directory and a name.
	// It calls GetRelativePath to generate the actual file path under given directory,
	Create(name string, state FileState) FileEntry

	// GetRelativePath returns the relative path for a file entry.
	// The path is relative to the state directory that file entry belongs to.
	// i.e. a file entry can have a relative path of 00/0e/filename under directory /var/cache/
	GetRelativePath(name string) string

	// ListNames lists all file entry names in state.
	ListNames(state FileState) ([]string, error)
}

// FileEntry manages one file and its metadata.
// It doesn't guarantee thread-safety; That should be handled by FileMap.
type FileEntry interface {
	GetState() FileState
	GetName() string
	GetPath() string
	GetStat() (os.FileInfo, error)

	Create(targetState FileState, len int64) error
	Reload(targetState FileState) error
	MoveFrom(targetState FileState, sourcePath string) error
	Move(targetState FileState) error
	MoveTo(targetPath string) error
	Delete() error

	GetReader() (FileReader, error)
	GetReadWriter() (FileReadWriter, error)

	AddMetadata(mt MetadataType) error
	GetMetadata(mt MetadataType) ([]byte, error)
	SetMetadata(mt MetadataType, data []byte) (bool, error)
	GetMetadataAt(mt MetadataType, b []byte, off int64) (int, error)
	SetMetadataAt(mt MetadataType, b []byte, off int64) (int, error)
	GetOrSetMetadata(mt MetadataType, b []byte) ([]byte, error)
	DeleteMetadata(mt MetadataType) error
	RangeMetadata(f func(mt MetadataType) error) error
}

var _ FileEntryFactory = (*localFileEntryFactory)(nil)
var _ FileEntryFactory = (*casFileEntryFactory)(nil)
var _ FileEntry = (*localFileEntry)(nil)

// localFileEntryFactory initializes localFileEntry obj.
type localFileEntryFactory struct{}

// NewLocalFileEntryFactory is the constructor for localFileEntryFactory.
func NewLocalFileEntryFactory() FileEntryFactory {
	return &localFileEntryFactory{}
}

// Create initializes and returns a FileEntry object.
func (f *localFileEntryFactory) Create(name string, state FileState) FileEntry {
	return newLocalFileEntry(state, name, f.GetRelativePath(name))
}

// GetRelativePath returns name because file entries are stored flat under state directory.
func (f *localFileEntryFactory) GetRelativePath(name string) string {
	return path.Join(name, DefaultDataFileName)
}

// ListNames returns the names of all entries in state's directory.
func (f *localFileEntryFactory) ListNames(state FileState) ([]string, error) {
	infos, err := ioutil.ReadDir(state.GetDirectory())
	if err != nil {
		return nil, err
	}
	var names []string
	for _, info := range infos {
		names = append(names, info.Name())
	}
	return names, nil
}

// casFileEntryFactory initializes localFileEntry obj.
// It uses the first few bytes of file digest (which is also used as file name) as shard ID.
// For every byte, one more level of directories will be created.
type casFileEntryFactory struct{}

// NewCASFileEntryFactory is the constructor for casFileEntryFactory.
func NewCASFileEntryFactory() FileEntryFactory {
	return &casFileEntryFactory{}
}

// Create initializes and returns a FileEntry object.
func (f *casFileEntryFactory) Create(name string, state FileState) FileEntry {
	return newLocalFileEntry(state, name, f.GetRelativePath(name))
}

// GetRelativePath returns content-addressable file path under state directory.
// Example:
// name = 07123e1f482356c415f684407a3b8723e10b2cbbc0b8fcd6282c49d37c9c1abc
// shardIDLength = 2
// relative path = 07/12/07123e1f482356c415f684407a3b8723e10b2cbbc0b8fcd6282c49d37c9c1abc
func (f *casFileEntryFactory) GetRelativePath(name string) string {
	filePath := ""
	for i := 0; i < int(DefaultShardIDLength) && i < len(name)/2; i++ {
		// (1 byte = 2 char of file name assumming file name is in HEX)
		dirName := name[i*2 : i*2+2]
		filePath = path.Join(filePath, dirName)
	}

	return path.Join(filePath, name, DefaultDataFileName)
}

// ListNames returns the names of all entries within the shards of state.
func (f *casFileEntryFactory) ListNames(state FileState) ([]string, error) {
	var names []string

	var readNames func(string, int) error
	readNames = func(dir string, depth int) error {
		infos, err := ioutil.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, info := range infos {
			if depth == 0 {
				names = append(names, info.Name())
			} else {
				if !info.IsDir() {
					continue
				}
				if err := readNames(path.Join(dir, info.Name()), depth-1); err != nil {
					return err
				}
			}
		}
		return nil
	}

	err := readNames(state.GetDirectory(), DefaultShardIDLength)

	return names, err
}

// localFileEntry implements FileEntry interface, handles IO operations for one file on local disk.
type localFileEntry struct {
	sync.RWMutex

	state            FileState
	name             string
	relativeDataPath string // Relative path to data file.
	metadataSet      map[MetadataType]struct{}
}

func newLocalFileEntry(
	state FileState,
	name string,
	relativeDataPath string,
) *localFileEntry {
	return &localFileEntry{
		state:            state,
		name:             name,
		relativeDataPath: relativeDataPath,
		metadataSet:      make(map[MetadataType]struct{}),
	}
}

// GetState returns current state of the file.
func (entry *localFileEntry) GetState() FileState {
	return entry.state
}

// GetName returns name of the file.
func (entry *localFileEntry) GetName() string {
	return entry.name
}

// GetPath returns current path of the file.
func (entry *localFileEntry) GetPath() string {
	return path.Join(entry.state.GetDirectory(), entry.relativeDataPath)
}

// GetStat returns a FileInfo describing the named file.
func (entry *localFileEntry) GetStat() (os.FileInfo, error) {
	return os.Stat(entry.GetPath())
}

// Create creates a file on disk.
func (entry *localFileEntry) Create(targetState FileState, size int64) error {
	if entry.state != targetState {
		return &FileStateError{
			Op:    "Create",
			Name:  entry.name,
			State: entry.state,
			Msg:   fmt.Sprintf("localFileEntry obj has state: %v", entry.state),
		}
	}

	// Verify if file was already created.
	targetPath := entry.GetPath()
	if _, err := os.Stat(targetPath); err == nil {
		return os.ErrExist
	}

	// Create dir.
	if err := os.MkdirAll(path.Dir(targetPath), DefaultDirPermission); err != nil {
		return err
	}

	// Create file.
	f, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Change size.
	err = f.Truncate(size)
	if err != nil {
		// Try to delete file.
		os.RemoveAll(path.Dir(targetPath))
		return err
	}

	return f.Close()
}

// Reload tries to reload a file that doesn't exist in memory from disk.
func (entry *localFileEntry) Reload(targetState FileState) error {
	// Verify the file is still on disk.
	if _, err := os.Stat(entry.GetPath()); err != nil {
		// Return os.ErrNotExist.
		return err
	}

	// Load metadata.
	files, err := ioutil.ReadDir(path.Dir(entry.GetPath()))
	if err != nil {
		return err
	}
	for _, currFile := range files {
		// Glob could return the data file itself, and directories.
		// Verify it's actually a metadata file.
		if currFile.Name() != DefaultDataFileName {
			mt := CreateFromSuffix(currFile.Name())
			if mt != nil {
				// Add metadata
				entry.AddMetadata(mt)
			}
		}
	}
	return nil
}

// MoveFrom moves an unmanaged file in.
func (entry *localFileEntry) MoveFrom(targetState FileState, sourcePath string) error {
	if entry.state != targetState {
		return &FileStateError{
			Op:    "MoveFrom",
			Name:  entry.name,
			State: entry.state,
			Msg:   fmt.Sprintf("localFileEntry obj has state: %v", entry.state),
		}
	}

	// Verify if file was already created.
	targetPath := entry.GetPath()
	if _, err := os.Stat(targetPath); err == nil {
		return os.ErrExist
	}

	// Verify the source file exists.
	if _, err := os.Stat(sourcePath); err != nil {
		// Return os.ErrNotExist.
		return err
	}

	// Create dir.
	if err := os.MkdirAll(path.Dir(targetPath), DefaultDirPermission); err != nil {
		return err
	}

	// Move data.
	return os.Rename(sourcePath, targetPath)
}

// Move moves file to target dir under the same name, moves all metadata that's `movable`, and
// updates state in memory.
// If for any reason the target path already exists, it will be overwritten.
func (entry *localFileEntry) Move(targetState FileState) error {
	sourcePath := entry.GetPath()
	targetPath := path.Join(targetState.GetDirectory(), entry.relativeDataPath)
	if err := os.MkdirAll(path.Dir(targetPath), DefaultDirPermission); err != nil {
		return err
	}

	// Get file stats, update size in memory.
	if _, err := os.Stat(sourcePath); err != nil {
		// Return os.ErrNotExist.
		return err
	}

	// Copy metadata first.
	performCopy := func(mt MetadataType) error {
		if mt.Movable() {
			sourceMetadataPath := entry.getMetadataPath(mt)
			targetMetadataPath := path.Join(path.Dir(targetPath), mt.GetSuffix())
			bytes, err := ioutil.ReadFile(sourceMetadataPath)
			if err != nil {
				return err
			}
			if _, err := compareAndWriteFile(targetMetadataPath, bytes); err != nil {
				return err
			}
		}
		return nil
	}
	if err := entry.RangeMetadata(performCopy); err != nil {
		return err
	}

	// Move data. This could be a slow operation if source and target are not on the same FS.
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	// Update parent dir in memory.
	entry.state = targetState

	// Delete source dir. Ignore error.
	os.RemoveAll(path.Dir(sourcePath))

	return nil
}

// MoveTo moves data file out to an unmanaged location.
func (entry *localFileEntry) MoveTo(targetPath string) error {
	sourcePath := entry.GetPath()

	// Create dir.
	if err := os.MkdirAll(path.Dir(targetPath), DefaultDirPermission); err != nil {
		return err
	}

	// Get file stats, update size in memory.
	if _, err := os.Stat(sourcePath); err != nil {
		// Return os.ErrNotExist.
		return err
	}

	// Move data.
	if err := os.Rename(sourcePath, targetPath); err != nil {
		return err
	}

	// Delete source dir. Ignore error.
	os.RemoveAll(path.Dir(sourcePath))
	return nil
}

// Delete removes file and all of its metedata files from disk.
func (entry *localFileEntry) Delete() error {
	// Remove files, ignore error.
	os.RemoveAll(path.Dir(entry.GetPath()))

	return nil
}

// GetReader returns a FileReader object for read operations.
func (entry *localFileEntry) GetReader() (FileReader, error) {
	f, err := os.OpenFile(entry.GetPath(), os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}

	reader := &localFileReadWriter{
		entry:      entry,
		descriptor: f,
	}
	return reader, nil
}

// GetReadWriter returns a FileReadWriter object for read/write operations.
func (entry *localFileEntry) GetReadWriter() (FileReadWriter, error) {
	f, err := os.OpenFile(entry.GetPath(), os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}

	readWriter := &localFileReadWriter{
		entry:      entry,
		descriptor: f,
	}
	return readWriter, nil
}

func (entry *localFileEntry) getMetadataPath(mt MetadataType) string {
	return path.Join(path.Dir(entry.GetPath()), mt.GetSuffix())
}

// AddMetadata adds a new metadata type to metadataSet. This is primirily used during reload.
func (entry *localFileEntry) AddMetadata(mt MetadataType) error {
	filePath := entry.getMetadataPath(mt)

	// Check existence.
	if _, err := os.Stat(filePath); err != nil {
		return err
	}
	entry.metadataSet[mt] = struct{}{}
	return nil
}

// GetMetadata returns metadata content as a byte array.
func (entry *localFileEntry) GetMetadata(mt MetadataType) ([]byte, error) {
	filePath := entry.getMetadataPath(mt)

	// Check existence.
	if _, err := os.Stat(filePath); err != nil {
		return nil, err
	}

	return ioutil.ReadFile(filePath)
}

// SetMetadata updates metadata and returns true only if the file is updated correctly;
// It returns false if error happened or file already contains desired content.
func (entry *localFileEntry) SetMetadata(mt MetadataType, b []byte) (bool, error) {
	filePath := entry.getMetadataPath(mt)

	updated, err := compareAndWriteFile(filePath, b)
	if err == nil {
		entry.metadataSet[mt] = struct{}{}
	}
	return updated, err
}

// GetMetadataAt reads metadata at specified offset.
func (entry *localFileEntry) GetMetadataAt(mt MetadataType, b []byte, off int64) (int, error) {
	filePath := entry.getMetadataPath(mt)

	// Check existence.
	if _, err := os.Stat(filePath); err != nil {
		return 0, err
	}

	// Read to data.
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return f.ReadAt(b, off)
}

// SetMetadataAt writes metadata at specified offset.
func (entry *localFileEntry) SetMetadataAt(mt MetadataType, b []byte, off int64) (int, error) {
	filePath := entry.getMetadataPath(mt)

	// Check existence.
	if _, err := os.Stat(filePath); err != nil {
		return 0, err
	}

	// Compare with existing data, overwrite if different.
	f, err := os.OpenFile(filePath, os.O_RDWR, 0755)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	buf := make([]byte, len(b))
	if _, err := f.ReadAt(buf, off); err != nil {
		return 0, err
	}
	if bytes.Compare(buf, b) == 0 {
		return 0, nil
	}

	return f.WriteAt(b, off)
}

// DeleteMetadata deletes metadata of the specified type.
func (entry *localFileEntry) DeleteMetadata(mt MetadataType) error {
	filePath := entry.getMetadataPath(mt)

	// Remove from map no matter if the actual metadata file is removed from disk.
	defer delete(entry.metadataSet, mt)

	return os.RemoveAll(filePath)
}

// RangeMetadata lofis through all metadata and applies function f, until an error happens.
func (entry *localFileEntry) RangeMetadata(f func(mt MetadataType) error) error {
	for mt := range entry.metadataSet {
		if err := f(mt); err != nil {
			return err
		}
	}
	return nil
}

// GetOrSetMetadata writes b under metadata mt if mt has not been initialized yet.
// Always returns the final content of the metadata, whether it be the existing content or the
// content just written.
func (entry *localFileEntry) GetOrSetMetadata(mt MetadataType, b []byte) ([]byte, error) {
	if _, ok := entry.metadataSet[mt]; ok {
		return entry.GetMetadata(mt)
	}
	filePath := path.Join(path.Dir(entry.GetPath()), mt.GetSuffix())
	if _, err := compareAndWriteFile(filePath, b); err != nil {
		return nil, err
	}
	entry.metadataSet[mt] = struct{}{}

	c := make([]byte, len(b))
	copy(c, b)
	return c, nil
}

// compareAndWriteFile updates file with given bytes and returns true only if the file is updated
// correctly.
// It returns false if error happened or file already contains desired content.
func compareAndWriteFile(filePath string, b []byte) (bool, error) {
	// Check existence.
	fs, err := os.Stat(filePath)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}

	if os.IsNotExist(err) {
		if err := os.MkdirAll(path.Dir(filePath), 0755); err != nil {
			return false, err
		}

		if err := ioutil.WriteFile(filePath, b, 0755); err != nil {
			return false, err
		}
		return true, nil
	}

	f, err := os.OpenFile(filePath, os.O_RDWR, 0755)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Compare with existing data, overwrite if different.
	buf := make([]byte, int(fs.Size()))
	if _, err := f.Read(buf); err != nil {
		return false, err
	}
	if bytes.Compare(buf, b) == 0 {
		return false, nil
	}

	if len(buf) != len(b) {
		if err := f.Truncate(int64(len(b))); err != nil {
			return false, err
		}
	}

	if _, err := f.WriteAt(b, 0); err != nil {
		return false, err
	}
	return true, nil
}
