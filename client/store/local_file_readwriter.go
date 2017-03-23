package store

import (
	"fmt"
	"io"
	"os"
)

// FileReader provides read operation on a file.
type FileReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
	Size() int64
}

// FileReadWriter provides read/write operation on a file.
type FileReadWriter interface {
	FileReader
	io.Writer
	io.WriterAt
}

// LocalFileReadWriter implements FileReadWriter interface, provides read/write operation on a
// local file.
type localFileReadWriter struct {
	entry      *localFileEntry
	descriptor *os.File
}

// Close decrements openCount, and closes underlying OS.File object if openCount reaches 0.
func (readWriter localFileReadWriter) Close() error {
	readWriter.entry.Lock()
	defer readWriter.entry.Unlock()

	if readWriter.entry.openCount < 1 {
		return fmt.Errorf("File %s is not open", readWriter.entry.name)
	}

	if err := readWriter.descriptor.Close(); err != nil {
		return err
	}

	readWriter.entry.openCount--
	return nil
}

// Write writes up to len(b) bytes to the File.
func (readWriter localFileReadWriter) Write(p []byte) (int, error) {
	return readWriter.descriptor.Write(p)
}

// WriteAt writes len(p) bytes from p to the underlying data stream at offset off.
func (readWriter localFileReadWriter) WriteAt(p []byte, offset int64) (int, error) {
	return readWriter.descriptor.WriteAt(p, offset)
}

// Read reads up to len(b) bytes from the File.
func (readWriter localFileReadWriter) Read(p []byte) (int, error) {
	return readWriter.descriptor.Read(p)
}

// ReadAt reads len(b) bytes from the File starting at byte offset off.
func (readWriter localFileReadWriter) ReadAt(p []byte, offset int64) (int, error) {
	return readWriter.descriptor.ReadAt(p, offset)
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted according to
// whence:
// 0 means relative to the origin of the file;
// 1 means relative to the current offset;
// 2 means relative to the end.
func (readWriter localFileReadWriter) Seek(offset int64, whence int) (int64, error) {
	return readWriter.descriptor.Seek(offset, whence)
}

// Size returns the size of the section in bytes.
func (readWriter localFileReadWriter) Size() int64 {
	info, err := readWriter.descriptor.Stat()
	if err != nil || info == nil {
		return 0
	}
	return info.Size()
}
