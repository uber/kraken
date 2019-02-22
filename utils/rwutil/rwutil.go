package rwutil

import "io"

// PlainReader provides an io.Reader for a bytes slice. It intentionally does
// not provide any other methods.
type PlainReader []byte

// Read always reads the entire underlying byte slice.
func (p PlainReader) Read(b []byte) (n int, err error) {
	copy(b, p)
	return len(p), io.EOF
}

// PlainWriter provides an io.Writer for a bytes slice. It intentionally does
// not provide any other methods. Clients should initialize length with make.
type PlainWriter []byte

// Write writes all of b to p.
func (p PlainWriter) Write(b []byte) (n int, err error) {
	copy(p, b)
	return len(p), nil
}
