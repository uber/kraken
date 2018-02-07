package storage

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"code.uber.internal/infra/kraken/lib/store"
)

type fileOpener func() (store.FileReader, error)

type filePieceReader struct {
	offset int64
	length int64

	open   fileOpener
	closer io.Closer
	reader io.Reader
}

// newFilePieceReader creates a filePieceReader which reads a piece from f. f should not
// be used once it is given to a filePieceReader.
func newFilePieceReader(
	offset, length int64, open fileOpener) *filePieceReader {

	return &filePieceReader{offset: offset, length: length, open: open}
}

func (r *filePieceReader) Read(b []byte) (int, error) {
	if r.reader == nil {
		f, err := r.open()
		if err != nil {
			return 0, fmt.Errorf("open: %s", err)
		}
		if _, err := f.Seek(r.offset, os.SEEK_SET); err != nil {
			return 0, fmt.Errorf("seek: %s", err)
		}
		r.reader = io.LimitReader(f, r.length)
		r.closer = f
	}
	return r.reader.Read(b)
}

func (r *filePieceReader) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}

func (r *filePieceReader) Length() int {
	return int(r.length)
}

type pieceReaderBuffer struct {
	reader *bytes.Reader
}

// NewPieceReaderBuffer returns a PieceReader which wraps an in-memory buffer.
func NewPieceReaderBuffer(b []byte) PieceReader {
	return &pieceReaderBuffer{bytes.NewReader(b)}
}

func (r *pieceReaderBuffer) Read(b []byte) (int, error) {
	return r.reader.Read(b)
}

func (r *pieceReaderBuffer) Close() error {
	return nil
}

func (r *pieceReaderBuffer) Length() int {
	return r.reader.Len()
}
