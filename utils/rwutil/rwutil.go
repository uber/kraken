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
