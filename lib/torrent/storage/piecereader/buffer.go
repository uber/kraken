package piecereader

import "bytes"

// Buffer is a storage.PieceReader which reads a piece from an in-memory buffer.
type Buffer struct {
	reader *bytes.Reader
}

// NewBuffer returns a new Buffer for b.
func NewBuffer(b []byte) *Buffer {
	return &Buffer{bytes.NewReader(b)}
}

// Read reads a piece into p.
func (b *Buffer) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

// Close noops.
func (b *Buffer) Close() error {
	return nil
}

// Length returns the length of the piece.
func (b *Buffer) Length() int {
	return b.reader.Len()
}
