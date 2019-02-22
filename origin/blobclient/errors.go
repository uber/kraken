package blobclient

import "errors"

// ErrBlobNotFound is returned when a blob is not found on origin.
var ErrBlobNotFound = errors.New("blob not found")
