package transfer

import "errors"

// ErrBlobNotFound is returned when a blob is not found by transferer.
var ErrBlobNotFound = errors.New("blob not found")
