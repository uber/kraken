// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package base

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/stringset"
)

// FileEntry errors.
var (
	ErrFilePersisted = errors.New("file is persisted")
	ErrInvalidName   = errors.New("invalid name")
)

// FileState decides what directory a file is in.
// A file can only be in one state at any given time.
type FileState struct {
	directory string
}

// NewFileState creates a new FileState for directory.
func NewFileState(directory string) FileState {
	return FileState{directory}
}

// GetDirectory returns the FileState's directory.
func (s FileState) GetDirectory() string {
	return s.directory
}

// FileEntryFactory initializes FileEntry obj.
type FileEntryFactory interface {
	// Create creates a file entry given a state directory and a name.
	// It calls GetRelativePath to generate the actual file path under given directory,
	Create(name string, state FileState) (FileEntry, error)

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
	Reload() error
	MoveFrom(targetState FileState, sourcePath string) error
	Move(targetState FileState) error
	LinkTo(targetPath string) error
	Delete() error

	GetReader() (FileReader, error)
	GetReadWriter() (FileReadWriter, error)

	AddMetadata(md metadata.Metadata) error

	GetMetadata(md metadata.Metadata) error
	SetMetadata(md metadata.Metadata) (bool, error)
	SetMetadataAt(md metadata.Metadata, b []byte, offset int64) (updated bool, err error)
	GetOrSetMetadata(md metadata.Metadata) error
	DeleteMetadata(md metadata.Metadata) error

	RangeMetadata(f func(md metadata.Metadata) error) error
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
func (f *localFileEntryFactory) Create(name string, state FileState) (FileEntry, error) {
	if name != filepath.Clean(name) {
		return nil, ErrInvalidName
	}
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") || strings.HasPrefix(name, "../") {
		return nil, ErrInvalidName
	}
	return newLocalFileEntry(state, name, f.GetRelativePath(name)), nil
}

// GetRelativePath returns name because file entries are stored flat under state directory.
func (f *localFileEntryFactory) GetRelativePath(name string) string {
	return filepath.Join(name, DefaultDataFileName)
}

// ListNames returns the names of all entries in state's directory.
func (f *localFileEntryFactory) ListNames(state FileState) ([]string, error) {
	var names []string

	var readNames func(string) error
	readNames = func(dir string) error {
		infos, err := ioutil.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		for _, info := range infos {
			if info.IsDir() {
				if err := readNames(filepath.Join(dir, info.Name())); err != nil {
					return err
				}
				continue
			}
			if info.Name() == DefaultDataFileName {
				name, err := filepath.Rel(state.GetDirectory(), dir)
				if err != nil {
					return err
				}
				names = append(names, name)
			}
		}
		return nil
	}

	err := readNames(state.GetDirectory())

	return names, err
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
// TODO: verify name.
func (f *casFileEntryFactory) Create(name string, state FileState) (FileEntry, error) {
	return newLocalFileEntry(state, name, f.GetRelativePath(name)), nil
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
		filePath = filepath.Join(filePath, dirName)
	}

	return filepath.Join(filePath, name, DefaultDataFileName)
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
				if err := readNames(filepath.Join(dir, info.Name()), depth-1); err != nil {
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
	relativeDataPath string        // Relative path to data file.
	metadata         stringset.Set // Metadata is identified by suffix.
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
		metadata:         make(stringset.Set),
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
	return filepath.Join(entry.state.GetDirectory(), entry.relativeDataPath)
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
	if err := os.MkdirAll(filepath.Dir(targetPath), DefaultDirPermission); err != nil {
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
		os.RemoveAll(filepath.Dir(targetPath))
		return err
	}

	return f.Close()
}

// Reload tries to reload a file that doesn't exist in memory from disk.
func (entry *localFileEntry) Reload() error {
	// Verify the file is still on disk.
	if _, err := os.Stat(entry.GetPath()); err != nil {
		// Return os.ErrNotExist.
		return err
	}

	// Load metadata.
	files, err := ioutil.ReadDir(filepath.Dir(entry.GetPath()))
	if err != nil {
		return err
	}
	for _, currFile := range files {
		// Glob could return the data file itself, and directories.
		// Verify it's actually a metadata file.
		if currFile.Name() != DefaultDataFileName {
			md := metadata.CreateFromSuffix(currFile.Name())
			if md != nil {
				// Add metadata
				entry.AddMetadata(md)
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
	if err := os.MkdirAll(filepath.Dir(targetPath), DefaultDirPermission); err != nil {
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
	targetPath := filepath.Join(targetState.GetDirectory(), entry.relativeDataPath)
	if err := os.MkdirAll(filepath.Dir(targetPath), DefaultDirPermission); err != nil {
		return err
	}

	// Get file stats.
	if _, err := os.Stat(sourcePath); err != nil {
		// Return os.ErrNotExist.
		return err
	}

	// Copy metadata first.
	performCopy := func(md metadata.Metadata) error {
		if md.Movable() {
			sourceMetadataPath := entry.getMetadataPath(md)
			targetMetadataPath := filepath.Join(filepath.Dir(targetPath), md.GetSuffix())
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

	// Delete source dir.
	return os.RemoveAll(filepath.Dir(sourcePath))
}

// LinkTo creates a hardlink to an unmanaged path.
func (entry *localFileEntry) LinkTo(targetPath string) error {
	// Create dir.
	if err := os.MkdirAll(filepath.Dir(targetPath), DefaultDirPermission); err != nil {
		return err
	}

	// Move data.
	return os.Link(entry.GetPath(), targetPath)
}

// Delete removes file and all of its metedata files from disk. If persist
// metadata is present and true, delete returns ErrFilePersisted.
func (entry *localFileEntry) Delete() error {
	var persist metadata.Persist
	if err := entry.GetMetadata(&persist); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("get persist metadata: %s", err)
		}
	} else {
		if persist.Value {
			return ErrFilePersisted
		}
	}

	// Remove files.
	return os.RemoveAll(filepath.Dir(entry.GetPath()))
}

// GetReader returns a FileReader object for read operations.
func (entry *localFileEntry) GetReader() (FileReader, error) {
	f, err := os.OpenFile(entry.GetPath(), os.O_RDONLY, 0775)
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
	f, err := os.OpenFile(entry.GetPath(), os.O_RDWR, 0775)
	if err != nil {
		return nil, err
	}

	readWriter := &localFileReadWriter{
		entry:      entry,
		descriptor: f,
	}
	return readWriter, nil
}

func (entry *localFileEntry) getMetadataPath(md metadata.Metadata) string {
	return filepath.Join(filepath.Dir(entry.GetPath()), md.GetSuffix())
}

// AddMetadata adds a new metadata type to metadata. This is primirily used during reload.
func (entry *localFileEntry) AddMetadata(md metadata.Metadata) error {
	filePath := entry.getMetadataPath(md)

	// Check existence.
	if _, err := os.Stat(filePath); err != nil {
		return err
	}
	entry.metadata.Add(md.GetSuffix())
	return nil
}

// GetMetadata reads and unmarshals metadata into md.
func (entry *localFileEntry) GetMetadata(md metadata.Metadata) error {
	filePath := entry.getMetadataPath(md)
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	return md.Deserialize(b)
}

// SetMetadata updates metadata and returns true only if the file is updated correctly.
// It returns false if error happened or file already contains desired content.
func (entry *localFileEntry) SetMetadata(md metadata.Metadata) (bool, error) {
	filePath := entry.getMetadataPath(md)
	b, err := md.Serialize()
	if err != nil {
		return false, fmt.Errorf("marshal metadata: %s", err)
	}
	updated, err := compareAndWriteFile(filePath, b)
	if err == nil {
		entry.metadata.Add(md.GetSuffix())
	}
	return updated, err
}

// SetMetadataAt overwrites a single byte of metadata. Returns true if the byte
// was overwritten.
func (entry *localFileEntry) SetMetadataAt(
	md metadata.Metadata, b []byte, offset int64) (updated bool, err error) {

	filePath := entry.getMetadataPath(md)
	f, err := os.OpenFile(filePath, os.O_RDWR, 0775)
	if err != nil {
		return false, err
	}
	defer f.Close()

	prev := make([]byte, len(b))
	if _, err := f.ReadAt(prev, offset); err != nil {
		return false, err
	}
	if bytes.Compare(prev, b) == 0 {
		return false, nil
	}
	if _, err := f.WriteAt(b, offset); err != nil {
		return false, err
	}
	return true, nil
}

// GetOrSetMetadata writes b under metadata md if md has not been initialized yet.
// If the given metadata is not initialized, md is overwritten.
func (entry *localFileEntry) GetOrSetMetadata(md metadata.Metadata) error {
	if entry.metadata.Has(md.GetSuffix()) {
		return entry.GetMetadata(md)
	}
	b, err := md.Serialize()
	if err != nil {
		return fmt.Errorf("marshal metadata: %s", err)
	}
	filePath := filepath.Join(filepath.Dir(entry.GetPath()), md.GetSuffix())
	if _, err := compareAndWriteFile(filePath, b); err != nil {
		return err
	}
	entry.metadata.Add(md.GetSuffix())
	return nil
}

// DeleteMetadata deletes metadata of the specified type.
func (entry *localFileEntry) DeleteMetadata(md metadata.Metadata) error {
	filePath := entry.getMetadataPath(md)

	// Remove from map no matter if the actual metadata file is removed from disk.
	defer entry.metadata.Remove(md.GetSuffix())

	return os.RemoveAll(filePath)
}

// RangeMetadata loops through all metadata and applies function f, until an error happens.
func (entry *localFileEntry) RangeMetadata(f func(md metadata.Metadata) error) error {
	for suffix := range entry.metadata {
		md := metadata.CreateFromSuffix(suffix)
		if md == nil {
			return fmt.Errorf("cannot create metadata from suffix %s", suffix)
		}
		if err := f(md); err != nil {
			return err
		}
	}
	return nil
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
		if err := os.MkdirAll(filepath.Dir(filePath), 0775); err != nil {
			return false, err
		}

		if err := ioutil.WriteFile(filePath, b, 0775); err != nil {
			return false, err
		}
		return true, nil
	}

	f, err := os.OpenFile(filePath, os.O_RDWR, 0775)
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
