package base

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
	case *FileStateError:
		return true
	default:
		return false
	}
}
