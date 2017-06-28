package refcountable

import "fmt"

// RefCountError represents errors related to ref count.
// It's used when trying to move/rename/delete a file that's still referenced.
type RefCountError struct {
	Op       string
	Name     string
	RefCount int64
	Msg      string
}

func (e *RefCountError) Error() string {
	return fmt.Sprintf("Failed to perform \"%s\" on %s with ref count %d: %s",
		e.Op, e.Name, e.RefCount, e.Msg)
}

// IsRefCountError returns true if the param is of RefCountError type.
func IsRefCountError(err error) bool {
	switch err.(type) {
	case *RefCountError:
		return true
	default:
		return false
	}
}
