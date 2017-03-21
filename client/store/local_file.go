package store

import (
	"fmt"
	"os"
	"sync"
)

// LocalFile keeps information of a downloaded file.
type LocalFile struct {
	sync.Mutex

	path      string
	name      string
	openCount int
}

// NewLocalFile returns a pointer and a new LocalFile object.
func NewLocalFile(path, name string) *LocalFile {
	return &LocalFile{
		path:      path,
		name:      name,
		openCount: 0,
	}
}

// isOpen() check if any caller still has this file open.
func (localFile *LocalFile) isOpen() bool {
	localFile.Lock()
	defer localFile.Unlock()

	if localFile.openCount > 0 {
		return true
	}
	return false
}

// LocalFileReader provides read operation on a file.
// It implements SectionReader and Closer interfaces.
type LocalFileReader struct {
	localFile  *LocalFile
	descriptor *os.File
}

// NewLocalFileReader returns a pointer and a new LocalFileReader object.
func NewLocalFileReader(localFile *LocalFile) (*LocalFileReader, error) {
	localFile.Lock()
	defer localFile.Unlock()

	f, err := os.Open(localFile.path)
	if err != nil {
		return nil, err
	}
	localFile.openCount++

	return &LocalFileReader{
		localFile:  localFile,
		descriptor: f,
	}, nil
}

// Close decrements openCount, and closes underlying OS.File object if openCount reaches 0.
func (reader *LocalFileReader) Close() error {
	reader.localFile.Lock()
	defer reader.localFile.Unlock()

	if reader.localFile.openCount < 1 {
		return fmt.Errorf("File %s is not open", reader.localFile.name)
	}

	if err := reader.descriptor.Close(); err != nil {
		return err
	}

	reader.localFile.openCount--
	return nil
}

// Read reads up to len(b) bytes from the File.
func (reader *LocalFileReader) Read(p []byte) (int, error) {
	return reader.descriptor.Read(p)
}

// ReadAt reads len(b) bytes from the File starting at byte offset off.
func (reader *LocalFileReader) ReadAt(p []byte, offset int64) (int, error) {
	return reader.descriptor.ReadAt(p, offset)
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted according to
// whence:
// 0 means relative to the origin of the file;
// 1 means relative to the current offset;
// 2 means relative to the end.
func (reader *LocalFileReader) Seek(offset int64, whence int) (int64, error) {
	return reader.descriptor.Seek(offset, whence)
}

// Size returns the size of the section in bytes.
func (reader *LocalFileReader) Size() int64 {
	info, err := reader.descriptor.Stat()
	if err != nil || info == nil {
		return 0
	}
	return info.Size()
}
