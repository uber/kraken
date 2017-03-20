package store

import (
	"fmt"
	"os"
	"sync"
)

// LocalFile is responsible for operations on a downloaded file.
// It implements SectionReader and Closer interfaces.
type LocalFile struct {
	sync.Mutex

	path      string
	name      string
	f         *os.File
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

// open initializes underlying OS.File object if it hasn't been, and increments openCount.
// It'is only supposed to be called through LocalFileStore.Get()
func (localFile *LocalFile) open() error {
	localFile.Lock()
	defer localFile.Unlock()

	if localFile.openCount > 0 && localFile.f != nil {
		localFile.openCount++
	} else if localFile.openCount == 0 {
		f, err := os.Open(localFile.path)
		if err != nil {
			return err
		}
		localFile.f = f
		localFile.openCount++
	} else {
		return fmt.Errorf("File %s has incorrect open count", localFile.name)
	}
	return nil
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

// Close decrements openCount, and closes underlying OS.File object if openCount reaches 0.
func (localFile *LocalFile) Close() error {
	localFile.Lock()
	defer localFile.Unlock()

	if localFile.openCount < 1 || localFile.f == nil {
		return fmt.Errorf("File %s is not open", localFile.name)
	} else if localFile.openCount == 1 {
		if err := localFile.f.Close(); err != nil {
			return err
		}
		localFile.openCount--
	} else {
		localFile.openCount--
	}
	return nil
}

// Read reads up to len(b) bytes from the File.
func (localFile *LocalFile) Read(p []byte) (int, error) {
	return localFile.f.Read(p)
}

// ReadAt reads len(b) bytes from the File starting at byte offset off.
func (localFile *LocalFile) ReadAt(p []byte, offset int64) (int, error) {
	return localFile.f.ReadAt(p, offset)
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted according to
// whence:
// 0 means relative to the origin of the file;
// 1 means relative to the current offset;
// 2 means relative to the end.
func (localFile *LocalFile) Seek(offset int64, whence int) (int64, error) {
	return localFile.f.Seek(offset, whence)
}

// Size returns the size of the section in bytes.
func (localFile *LocalFile) Size() int64 {
	info, err := localFile.f.Stat()
	if err != nil || info == nil {
		return 0
	}
	return info.Size()
}
