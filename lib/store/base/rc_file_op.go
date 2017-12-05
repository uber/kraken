package base

import (
	"encoding/binary"
	"fmt"
	"os"
)

// RCFileOp performs one file or metadata operation on FileStore, given a list of acceptable states.
type RCFileOp interface {
	FileOp
	GetFileRefCount(fileName string) (int64, error)
	IncFileRefCount(fileName string) (int64, error)
	DecFileRefCount(fileName string) (int64, error)
}

// LocalRCFileOp expands localFileOp.
type LocalRCFileOp struct {
	*localFileOp
}

// NewLocalRCFileOp inits a new RCFileOp obj.
func NewLocalRCFileOp(op *localFileOp) RCFileOp {
	return &LocalRCFileOp{
		localFileOp: op,
	}
}

// AcceptState adds a new state to the acceptable states list.
func (op *LocalRCFileOp) AcceptState(state FileState) FileOp {
	op.states[state] = struct{}{}
	return op
}

// GetAcceptableStates returns a set of acceptable states.
func (op *LocalRCFileOp) GetAcceptableStates() map[FileState]interface{} {
	return op.states
}

// getRefCountHelper returns current ref count. No ref count file means ref count is 0.
func getRefCountHelper(entry FileEntry) (int64, error) {
	// Read value.
	var refCount int64
	var n int
	b, err := entry.GetMetadata(NewRefCount())
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

func updateRefCountHelper(entry FileEntry, increment bool) (int64, error) {
	refCount, err := getRefCountHelper(entry)
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

	_, err = entry.SetMetadata(NewRefCount(), buf)
	if err != nil {
		return 0, err
	}

	return refCount, nil
}

// MoveFileTo checks ref count, then moves file to unmanaged location.
func (op *LocalRCFileOp) MoveFileTo(name, targetPath string) (err error) {
	if loadErr := op.deleteHelper(name, func(name string, entry FileEntry) error {
		var refCount int64
		refCount, err = getRefCountHelper(entry)
		if err != nil || refCount > 0 {
			// Prevent entry from being moved out.
			err = &RefCountError{
				Op:       "MoveTo",
				Name:     name,
				RefCount: refCount,
				Msg:      "file still referenced"}
			return err
		}
		err = entry.MoveTo(targetPath)
		return nil
	}); loadErr != nil {
		return loadErr
	}
	return err
}

// DeleteFile checks ref count, then removes file and all of its metedata files from disk.
func (op *LocalRCFileOp) DeleteFile(name string) (err error) {
	if loadErr := op.deleteHelper(name, func(name string, entry FileEntry) error {
		var refCount int64
		refCount, err = getRefCountHelper(entry)
		if err != nil || refCount > 0 {
			// Prevent entry from being deleted.
			err = &RefCountError{
				Op:       "DeleteFile",
				Name:     name,
				RefCount: refCount,
				Msg:      "file still referenced"}
			return err
		}
		err = entry.Delete()
		return nil
	}); loadErr != nil {
		return loadErr
	}
	return err
}

// GetFileRefCount retrieves file ref count. Ref count is stored in a metadata file on local disk.
func (op *LocalRCFileOp) GetFileRefCount(name string) (refCount int64, err error) {
	if loadErr := op.loadHelper(name, true, func(name string, entry FileEntry) {
		refCount, err = getRefCountHelper(entry)
	}); loadErr != nil {
		return 0, loadErr
	}
	return refCount, err
}

// IncFileRefCount increments file ref count. Ref count is stored in a metadata file on local disk.
func (op *LocalRCFileOp) IncFileRefCount(name string) (refCount int64, err error) {
	if loadErr := op.loadHelper(name, false, func(name string, entry FileEntry) {
		refCount, err = updateRefCountHelper(entry, true)
	}); loadErr != nil {
		return 0, loadErr
	}
	return refCount, err
}

// DecFileRefCount decrements file ref count. Ref count is stored in a metadata file on local disk.
func (op *LocalRCFileOp) DecFileRefCount(name string) (refCount int64, err error) {
	if loadErr := op.loadHelper(name, false, func(name string, entry FileEntry) {
		refCount, err = updateRefCountHelper(entry, false)
	}); loadErr != nil {
		return 0, loadErr
	}
	return refCount, err
}
