package tiered

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/lib/store/memory"
	"github.com/uber/kraken/utils/closers"
	"go.uber.org/zap"
)

var _ storelib.FileReadWriter = &File{}

// File represents an open handle to a blob in [Store], similar to how an [os.File] is an open file descriptor to a file on disk.
// File, just like a Linux file with its page cache, continues to work seemlessly if the blob is initially in memory, but later flushed to disk and evicted from memory.
type File struct {
	memF      *memory.File
	diskF     *disk.File
	key       string
	diskStore *disk.Store
	openErr   error
	once      sync.Once
	log       *zap.SugaredLogger
}

// Either [memory.File] OR [disk.File] must be set, but not both. If disk.File is not set, it will be lazily initialized
// on File's first call after the blob is evicted from memory.
func newFile(key string, memF *memory.File, diskF *disk.File, diskStore *disk.Store, log *zap.SugaredLogger) *File {
	f := &File{
		memF:      memF,
		diskF:     nil,
		key:       key,
		diskStore: diskStore,
		log:       log,
		openErr:   nil,
	}
	if diskF != nil {
		f.once.Do(func() { f.diskF = diskF })
	}
	return f
}

// A blob might be evicted from the mem store (after being flushed to disk), while a tiered store user is holding a [File].
// To allow the user to continue operating on the [File] seemlessly, we open the blob in the disk store and
// operate on it instead. This is done once, on the first call to [File] after eviction from memory.
func (f *File) openDiskFileIfNeeed() error {
	f.once.Do(func() {
		diskF, err := f.diskStore.Open(f.key)
		if errors.Is(err, os.ErrNotExist) {
			f.openErr = errBadSwitch(errors.New("blob not found in neither memory store nor disk store"))
			f.log.With("key", f.key).
				Error("invariant violation - a blob is neither memory.Store nor disk.Store while a tiered.Store user is trying to operate on the file")
			return
		}
		if err != nil {
			f.openErr = errBadSwitch(fmt.Errorf("disk store open: %w", err))
			f.log.With(
				"key", f.key,
				"error", f.openErr).
				Error("tiered.Store user could not operate on a blob, as the tiered.File could not transition from pointing at memory.File to pointing at disk.File after blob eviction from memory store")
			return
		}
		_, err = diskF.Seek(f.memF.Off(), io.SeekStart)
		if err != nil {
			closers.Close(diskF)
			f.openErr = errBadSwitch(fmt.Errorf("disk file seek: %w", err))
			f.log.With(
				"key", f.key,
				"error", f.openErr).
				Error("tiered.Store user could not operate on a blob, as the tiered.File could not transition from pointing at memory.File to pointing at disk.File after blob eviction from memory store")
			return
		}

		f.diskF = diskF
	})

	return f.openErr
}

func errBadSwitch(subErr error) error {
	return fmt.Errorf("tiered.File could not switch over from pointing to memory.File to pointing to disk.File after blob was evicted from memory: %w", subErr)
}

func (f *File) Read(p []byte) (n int, err error) {
	if f.memF != nil {
		n, err = f.memF.Read(p)
		if err != memory.ErrEvicted {
			return n, err
		}

		if err := f.openDiskFileIfNeeed(); err != nil {
			return 0, err
		}
	}
	return f.diskF.Read(p)
}

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	if f.memF != nil {
		n, err = f.memF.ReadAt(p, off)
		if err != memory.ErrEvicted {
			return n, err
		}
		if err := f.openDiskFileIfNeeed(); err != nil {
			return 0, err
		}
	}
	return f.diskF.ReadAt(p, off)
}

func (f *File) Seek(off int64, whence int) (int64, error) {
	if f.memF != nil {
		newOff, err := f.memF.Seek(off, whence)
		if err != memory.ErrEvicted {
			return newOff, err
		}

		if err := f.openDiskFileIfNeeed(); err != nil {
			return 0, err
		}
	}
	return f.diskF.Seek(off, whence)
}

func (f *File) Size() int64 {
	if f.memF != nil {
		size := f.memF.Size()
		if size != -1 {
			// Same as [memory.ErrEvicted].
			return size
		}

		if err := f.openDiskFileIfNeeed(); err != nil {
			return 0
		}
	}
	return f.diskF.Size()
}

func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	if f.memF != nil {
		n, err = f.memF.WriteAt(p, off)
		if err != memory.ErrEvicted {
			return n, err
		}

		if err := f.openDiskFileIfNeeed(); err != nil {
			return 0, err
		}
	}
	return f.diskF.WriteAt(p, off)
}

func (f *File) Write(p []byte) (n int, err error) {
	if f.memF != nil {
		n, err = f.memF.Write(p)
		if err != memory.ErrEvicted {
			return n, err
		}

		if err := f.openDiskFileIfNeeed(); err != nil {
			return 0, err
		}
	}
	return f.diskF.Write(p)
}

func (f *File) Cancel() error {
	if f.memF != nil {
		err := f.memF.Cancel()
		if err != memory.ErrEvicted {
			return err
		}

		if err := f.openDiskFileIfNeeed(); err != nil {
			return err
		}
	}
	return f.diskF.Cancel()
}
func (f *File) Close() error {
	if f.memF != nil {
		err := f.memF.Close()
		if err != memory.ErrEvicted {
			return err
		}

		if err := f.openDiskFileIfNeeed(); err != nil {
			return err
		}
	}
	return f.diskF.Close()

}
func (f *File) Commit() error {
	if f.memF != nil {
		err := f.memF.Commit()
		if err != memory.ErrEvicted {
			return err
		}

		if err := f.openDiskFileIfNeeed(); err != nil {
			return err
		}
	}
	return f.diskF.Commit()
}
