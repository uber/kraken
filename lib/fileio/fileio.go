package fileio

import "io"

// Reader defines read methods for file io.
type Reader interface {
	io.Reader
	io.ReaderAt
}

// Writer defines write methods for file io.
type Writer interface {
	io.Writer
	io.WriterAt
}

// ReadWriter defines read and write methods for file io.
type ReadWriter interface {
	Reader
	Writer
}
