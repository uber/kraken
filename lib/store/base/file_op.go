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
	"fmt"
	"os"
	"strings"

	"github.com/uber/kraken/lib/store/metadata"
)

type lockLevel int

const (
	// lockLevelPeek indicates lock for peek.
	_lockLevelPeek lockLevel = iota
	// lockLevelRead indicates lock for read.
	_lockLevelRead
	// lockLevelWrite indicates lock for read.
	_lockLevelWrite
)

// FileOp performs one file or metadata operation on FileStore, given a list of
// acceptable states.
type FileOp interface {
	AcceptState(state FileState) FileOp
	GetAcceptableStates() map[FileState]interface{}

	CreateFile(name string, createState FileState, len int64) error
	MoveFileFrom(name string, createState FileState, sourcePath string) error
	MoveFile(name string, goalState FileState) error
	LinkFileTo(name string, targetPath string) error
	DeleteFile(name string) error

	GetFilePath(name string) (string, error)
	GetFileStat(name string) (os.FileInfo, error)

	GetFileReader(name string) (FileReader, error)
	GetFileReadWriter(name string) (FileReadWriter, error)

	GetFileMetadata(name string, md metadata.Metadata) error
	SetFileMetadata(name string, md metadata.Metadata) (bool, error)
	SetFileMetadataAt(name string, md metadata.Metadata, b []byte, offset int64) (bool, error)
	GetOrSetFileMetadata(name string, md metadata.Metadata) error
	DeleteFileMetadata(name string, md metadata.Metadata) error

	RangeFileMetadata(name string, f func(metadata.Metadata) error) error

	ListNames() ([]string, error)

	String() string
}

var _ FileOp = (*localFileOp)(nil)

// localFileOp is a short-lived obj that performs one file or metadata operation
// on local disk, given a list of acceptable states.
type localFileOp struct {
	s      *localFileStore
	states map[FileState]interface{} // Set of states that's acceptable.
}

// NewLocalFileOp inits a new FileOp obj.
func NewLocalFileOp(s *localFileStore) FileOp {
	return &localFileOp{
		s:      s,
		states: make(map[FileState]interface{}),
	}
}

// AcceptState adds a new state to the acceptable states list.
func (op *localFileOp) AcceptState(state FileState) FileOp {
	op.states[state] = struct{}{}
	return op
}

// GetAcceptableStates returns a set of acceptable states.
func (op *localFileOp) GetAcceptableStates() map[FileState]interface{} {
	return op.states
}

// verifyStateHelper verifies file is in one of the acceptable states.
func (op *localFileOp) verifyStateHelper(name string, entry FileEntry) error {
	currState := entry.GetState()
	for state := range op.states {
		if currState == state {
			// File is in one of the acceptable states.
			return nil
		}
	}
	return &FileStateError{
		Op:    "verifyStateHelper",
		Name:  name,
		State: currState,
		Msg:   fmt.Sprintf("desired states: %v", op.states),
	}
}

// reloadFileEntryHelper tries to reload file from disk into memory.
// Note it doesn't try to verify states or reload file from all possible states.
// If reload succeeded, return true;
// If file already exists in memory, return false;
// If file is neither in memory or on disk, return false with os.ErrNotExist.
func (op *localFileOp) reloadFileEntryHelper(name string) (reloaded bool, err error) {
	if op.s.fileMap.Contains(name) {
		return false, nil
	}

	// Check if file exists on disk.
	// TODO: The states need to be guaranteed to be topologically sorted.
	for state := range op.states {
		fileEntry, err := op.s.fileEntryFactory.Create(name, state)
		if err != nil {
			return false, fmt.Errorf("create: %s", err)
		}

		// Try load before acquiring lock first.
		if err = fileEntry.Reload(); err != nil {
			continue
		}
		// Try to store file entry into memory.
		if stored := op.s.fileMap.TryStore(name, fileEntry, func(name string, entry FileEntry) bool {
			// Verify the file is still on disk.
			err = entry.Reload()
			return err == nil
		}); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false, err
		} else if !stored {
			// The entry was just reloaded by another goroutine, return true.
			// Since TryStore() updates LAT of existing entry, it's unlikely
			// that the entry would be deleted before this function returns.
			return true, nil
		}
		return true, nil
	}
	return false, os.ErrNotExist
}

// lockHelper runs f under protection of entry level RWMutex.
func (op *localFileOp) lockHelper(
	name string, l lockLevel, f func(name string, entry FileEntry)) (err error) {
	if _, err = op.reloadFileEntryHelper(name); err != nil {
		return err
	}
	var loaded bool
	if l == _lockLevelPeek {
		loaded = op.s.fileMap.LoadForPeek(name, func(name string, entry FileEntry) {
			if err = op.verifyStateHelper(name, entry); err != nil {
				return
			}
			f(name, entry)
		})
	} else if l == _lockLevelRead {
		loaded = op.s.fileMap.LoadForRead(name, func(name string, entry FileEntry) {
			if err = op.verifyStateHelper(name, entry); err != nil {
				return
			}
			f(name, entry)
		})
	} else if l == _lockLevelWrite {
		loaded = op.s.fileMap.LoadForWrite(name, func(name string, entry FileEntry) {
			if err = op.verifyStateHelper(name, entry); err != nil {
				return
			}
			f(name, entry)
		})
	}
	if !loaded {
		return os.ErrNotExist
	}
	return err
}

func (op *localFileOp) deleteHelper(
	name string, f func(name string, entry FileEntry) bool) (err error) {
	if _, err = op.reloadFileEntryHelper(name); err != nil {
		return err
	}
	op.s.fileMap.Delete(name, func(name string, entry FileEntry) bool {
		err = op.verifyStateHelper(name, entry)
		if err != nil {
			return false
		}

		return f(name, entry)
	})
	return err
}

// createFileHelper is a helper function that adds a new file to store.
// it either moves the new file from a unmanaged location, or creates an empty
// file with specified size.
// If file exists and is in an acceptable state, returns os.ErrExist.
// If file exists but not in an acceptable state, returns FileStateError.
func (op *localFileOp) createFileHelper(
	name string, targetState FileState, sourcePath string, len int64) (err error) {
	// Check if file exists in in-memory map and is in an acceptable state.
	loaded := op.s.fileMap.LoadForRead(name, func(name string, entry FileEntry) {
		err = op.verifyStateHelper(name, entry)
	})
	if err != nil && !os.IsNotExist(err) {
		// Includes FileStateError.
		return err
	} else if loaded {
		return os.ErrExist
	}

	// Check if file is on disk.
	loaded, err = op.reloadFileEntryHelper(name)
	if err != nil && !os.IsNotExist(err) {
		// Includes FileStateError.
		return err
	} else if loaded {
		return os.ErrExist
	}

	// Create new file entry.
	err = nil
	newEntry, err := op.s.fileEntryFactory.Create(name, targetState)
	if err != nil {
		return fmt.Errorf("create: %s", err)
	}
	if stored := op.s.fileMap.TryStore(name, newEntry, func(name string, entry FileEntry) bool {
		if sourcePath != "" {
			err = newEntry.MoveFrom(targetState, sourcePath)
			if err != nil {
				return false
			}
		} else {
			err = newEntry.Create(targetState, len)
			if err != nil {
				return false
			}
		}
		return true
	}); err != nil {
		return err
	} else if !stored {
		// Another goroutine created the entry before this one, verify again for
		// correct error message.
		// Since TryStore() updates LAT of existing entry, it's unlikely that
		// the entry would be deleted before this function returns.
		if loadErr := op.lockHelper(name, _lockLevelRead, func(name string, entry FileEntry) {
			return
		}); loadErr != nil {
			return loadErr
		}
		return os.ErrExist
	}

	return nil
}

// CreateFile creates an empty file with specified size.
// If file exists and is in an acceptable state, returns os.ErrExist.
// If file exists but not in an acceptable state, returns FileStateError.
func (op *localFileOp) CreateFile(name string, targetState FileState, len int64) (err error) {
	return op.createFileHelper(name, targetState, "", len)
}

// MoveFileFrom moves an unmanaged file into file store.
// If file exists and is in an acceptable state, returns os.ErrExist.
// If file exists but not in an acceptable state, returns FileStateError.
func (op *localFileOp) MoveFileFrom(name string, targetState FileState, sourcePath string) (err error) {
	return op.createFileHelper(name, targetState, sourcePath, -1)
}

// MoveFile moves a file to a different directory and updates its state
// accordingly, and moves all metadata that's `movable`.
func (op *localFileOp) MoveFile(name string, targetState FileState) (err error) {
	if _, err = op.reloadFileEntryHelper(name); err != nil {
		return err
	}

	// Verify that the file is not in target state, and is currently in one of
	// the acceptable states.
	loaded := op.s.fileMap.LoadForWrite(name, func(name string, entry FileEntry) {
		currState := entry.GetState()
		if currState == targetState {
			err = os.ErrExist
			return
		}
		for state := range op.states {
			if currState == state {
				// File is in one of the acceptable states. Perform move.
				err = entry.Move(targetState)
				return
			}
		}
		err = &FileStateError{
			Op:    "MoveFile",
			State: currState,
			Name:  name,
			Msg:   fmt.Sprintf("desired states: %v", op.states),
		}
	})
	if !loaded {
		return os.ErrNotExist
	}
	return err
}

// LinkFileTo create a hardlink to an unmanaged path.
func (op *localFileOp) LinkFileTo(name string, targetPath string) (err error) {
	if loadErr := op.lockHelper(name, _lockLevelRead, func(name string, entry FileEntry) {
		err = entry.LinkTo(targetPath)
	}); loadErr != nil {
		return loadErr
	}
	return err
}

// DeleteFile removes a file from disk and file map.
func (op *localFileOp) DeleteFile(name string) (err error) {
	if loadErr := op.deleteHelper(name, func(name string, entry FileEntry) bool {
		err = entry.Delete()
		// Return true so the entry would be removed from map regardless.
		return true
	}); loadErr != nil {
		return loadErr
	}
	return err
}

// GetFilePath returns full path for a file.
func (op *localFileOp) GetFilePath(name string) (path string, err error) {
	if loadErr := op.lockHelper(name, _lockLevelPeek, func(name string, entry FileEntry) {
		path = entry.GetPath()
	}); loadErr != nil {
		return "", loadErr
	}
	return path, nil
}

// GetFileStat returns FileInfo for a file.
func (op *localFileOp) GetFileStat(name string) (info os.FileInfo, err error) {
	if loadErr := op.lockHelper(name, _lockLevelPeek, func(name string, entry FileEntry) {
		info, err = entry.GetStat()
	}); loadErr != nil {
		return nil, loadErr
	}
	return info, err
}

// GetFileReader returns a FileReader object for read operations.
func (op *localFileOp) GetFileReader(name string) (r FileReader, err error) {
	if loadErr := op.lockHelper(name, _lockLevelRead, func(name string, entry FileEntry) {
		r, err = entry.GetReader()
	}); loadErr != nil {
		return nil, loadErr
	}
	return r, err
}

// GetFileReadWriter returns a FileReadWriter object for read/write operations.
func (op *localFileOp) GetFileReadWriter(name string) (w FileReadWriter, err error) {
	if loadErr := op.lockHelper(name, _lockLevelWrite, func(name string, entry FileEntry) {
		w, err = entry.GetReadWriter()
	}); loadErr != nil {
		return nil, loadErr
	}
	return w, err
}

// GetFileMetadata loads metadata assocciated with the file.
func (op *localFileOp) GetFileMetadata(name string, md metadata.Metadata) (err error) {
	if loadErr := op.lockHelper(name, _lockLevelPeek, func(name string, entry FileEntry) {
		err = entry.GetMetadata(md)
	}); loadErr != nil {
		return loadErr
	}
	return err
}

// SetFileMetadata creates or overwrites metadata assocciate with the file.
func (op *localFileOp) SetFileMetadata(name string, md metadata.Metadata) (updated bool, err error) {
	if loadErr := op.lockHelper(name, _lockLevelWrite, func(name string, entry FileEntry) {
		updated, err = entry.SetMetadata(md)
	}); loadErr != nil {
		return false, loadErr
	}
	return updated, err
}

// SetFileMetadataAt overwrites metadata assocciate with the file with content.
func (op *localFileOp) SetFileMetadataAt(
	name string, md metadata.Metadata, b []byte, offset int64) (updated bool, err error) {

	if loadErr := op.lockHelper(name, _lockLevelWrite, func(name string, entry FileEntry) {
		updated, err = entry.SetMetadataAt(md, b, offset)
	}); loadErr != nil {
		return false, loadErr
	}
	return updated, err
}

// GetOrSetFileMetadata see localFileEntryInternal.
func (op *localFileOp) GetOrSetFileMetadata(name string, md metadata.Metadata) (err error) {
	if loadErr := op.lockHelper(name, _lockLevelWrite, func(name string, entry FileEntry) {
		err = entry.GetOrSetMetadata(md)
	}); loadErr != nil {
		return loadErr
	}
	return err
}

// DeleteFileMetadata deletes metadata of the specified type for a file.
func (op *localFileOp) DeleteFileMetadata(name string, md metadata.Metadata) (err error) {
	loadErr := op.lockHelper(name, _lockLevelWrite, func(name string, entry FileEntry) {
		err = entry.DeleteMetadata(md)
	})
	if loadErr != nil {
		return loadErr
	}
	return err
}

// RangeFileMetadata loops through all metadata of one file and applies function f, until an error happens.
func (op *localFileOp) RangeFileMetadata(name string, f func(md metadata.Metadata) error) (err error) {
	loadErr := op.lockHelper(name, _lockLevelWrite, func(name string, entry FileEntry) {
		err = entry.RangeMetadata(f)
	})
	if loadErr != nil {
		return loadErr
	}
	return err
}

func (op *localFileOp) ListNames() ([]string, error) {
	var names []string
	for state := range op.states {
		stateNames, err := op.s.fileEntryFactory.ListNames(state)
		if err != nil {
			return nil, err
		}
		names = append(names, stateNames...)
	}
	return names, nil
}

func (op *localFileOp) String() string {
	var dirs []string
	for state := range op.states {
		dirs = append(dirs, state.GetDirectory())
	}
	return fmt.Sprintf("{%s}", strings.Join(dirs, ", "))
}
