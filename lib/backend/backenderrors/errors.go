package backenderrors

import "errors"

// ErrBlobNotFound is returned when a blob is not found in a storage backend.
var ErrBlobNotFound = errors.New("blob not found")
