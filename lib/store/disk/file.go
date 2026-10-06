package disk

import (
	"os"

	storelib "github.com/uber/kraken/lib/store"
)

func newFile(f *os.File) *File {
	return &File{
		fd: f,
	}
}

var _ storelib.FileReadWriter = &File{}

// File represends an open file descriptor to a blob in [Store].
type File struct {
	fd *os.File
}

func (f *File) Read(p []byte) (n int, err error)               { return f.fd.Read(p) }
func (f *File) ReadAt(p []byte, off int64) (n int, err error)  { return f.fd.ReadAt(p, off) }
func (f *File) Seek(off int64, whence int) (int64, error)      { return f.fd.Seek(off, whence) }
func (f *File) Write(p []byte) (n int, err error)              { return f.fd.Write(p) }
func (f *File) WriteAt(p []byte, off int64) (n int, err error) { return f.fd.WriteAt(p, off) }
func (f *File) Close() error                                   { return f.fd.Close() }

// Size returns the number of bytes the file contains.
func (f *File) Size() int64 {
	info, err := f.fd.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// Cancel is supposed to remove any written content.
// In this implementation file is not actually removed, but rather closed, which is fine, as there won't be key collisions when creating new files.
func (f *File) Cancel() error {
	return f.fd.Close()
}

// Commit is supposed to flush all content for buffered writer.
func (f *File) Commit() error {
	// TODO - consider whether we should do f.Sync() before closing the file.
	return f.fd.Close()
}
