package store

import "fmt"

// FileStateError represents errors related to file state.
// It's used when a file is not in the state it was supposed to be in.
type FileStateError struct {
	Op    string
	State FileState
	Name  string
	Msg   string
}

func (e *FileStateError) Error() string {
	return fmt.Sprintf("Failed to perform \"%s\" on %s/%s: %s",
		e.Op, e.State.GetDirectory(), e.Name, e.Msg)
}

// IsFileStateError returns true if the param is of FileStateError type.
func IsFileStateError(err error) bool {
	switch err.(type) {
	default:
		return false
	case *FileStateError:
		return true
	}
}

// RefCountError represents errors related to ref count.
// It's used when trying to move/rename/delete a file that's still referenced.
type RefCountError struct {
	Op       string
	State    FileState
	Name     string
	RefCount int64
	Msg      string
}

func (e *RefCountError) Error() string {
	return fmt.Sprintf("Failed to perform \"%s\" on %s/%s with ref count %d: %s",
		e.Op, e.State.GetDirectory(), e.Name, e.RefCount, e.Msg)
}

// IsRefCountError returns true if the param is of RefCountError type.
func IsRefCountError(err error) bool {
	switch err.(type) {
	default:
		return false
	case *RefCountError:
		return true
	}
}
