package internal

import "fmt"

// FileStateError represents errors related to file state.
// It's used when a file is not in the state it was supposed to be in.
type FileStateError struct {
	Op    string
	Name  string
	State FileState
	Msg   string
}

func (e *FileStateError) Error() string {
	return fmt.Sprintf("failed to perform \"%s\" on %s/%s: %s",
		e.Op, e.State.GetDirectory(), e.Name, e.Msg)
}

// IsFileStateError returns true if the param is of FileStateError type.
func IsFileStateError(err error) bool {
	_, ok := err.(*FileStateError)
	return ok
}

// RefCountError represents errors related to ref count.
// It's used when trying to move/rename/delete a file that's still referenced.
type RefCountError struct {
	Op       string
	Name     string
	RefCount int64
	Msg      string
}

func (e *RefCountError) Error() string {
	return fmt.Sprintf("failed to perform \"%s\" on %s with ref count %d: %s",
		e.Op, e.Name, e.RefCount, e.Msg)
}

// IsRefCountError returns true if the param is of RefCountError type.
func IsRefCountError(err error) bool {
	_, ok := err.(*RefCountError)
	return ok
}
