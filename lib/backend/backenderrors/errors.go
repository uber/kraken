package backenderrors

import "errors"

// ErrBlobNotFound is returned when a blob is not found in a storage backend.
var ErrBlobNotFound = errors.New("blob not found")

// ErrDirNotFound is returned when a dir is not found in a storage backend.
var ErrDirNotFound = errors.New("dir not found")
