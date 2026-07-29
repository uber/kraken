package memory

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	storelib "github.com/uber/kraken/lib/store"
)

var _ storelib.FileReadWriter = &File{}

// File represents an open handle to a blob in [Store], similar to how an [os.File] is an open file descriptor to a file on disk.
// As soon as the blob is evicted, File's APIs starts returning [ErrEvicted], as File no longer has a reference to its data, ensuring GC can clean it.
type File struct {
	data    *atomic.Pointer[[]byte]
	sliceMu *sync.Mutex // A potential optimization if contention is too high: currently no writes are parallelized. However, writes that only mutate the array but not the slice are parallelizable with each other. Thus, we could transition to a RWMutex to enable that.
	off     int64
}

func newFile(data *atomic.Pointer[[]byte], sliceMu *sync.Mutex) *File {
	return &File{
		data:    data,
		sliceMu: sliceMu,
		off:     0,
	}
}

func (f *File) getData() (data []byte, evicted bool) {
	buf := f.data.Load()
	if buf == nil {
		return nil, true
	}
	return *buf, false
}

func (f *File) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	buf, evicted := f.getData()
	if evicted {
		return 0, ErrEvicted
	}

	if f.off >= int64(len(buf)) {
		return 0, io.EOF
	}
	n = copy(p, buf[f.off:])
	f.off += int64(n)
	return n, nil
}

// ReadAt implements [io.ReaderAt]. Thread-safe.
func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, errors.New("negative offset")
	}

	buf, evicted := f.getData()
	if evicted {
		return 0, ErrEvicted
	}
	if off >= int64(len(buf)) {
		return 0, io.EOF
	}

	n = copy(p, buf[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Seek implements [io.Seeker]. Not thread-safe.
func (f *File) Seek(off int64, whence int) (int64, error) {
	buf, evicted := f.getData()
	if evicted {
		return 0, ErrEvicted
	}

	var newOff int64
	switch whence {
	case io.SeekStart:
		newOff = off
	case io.SeekCurrent:
		newOff = f.off + off
	case io.SeekEnd:
		newOff = int64(len(buf)) + off
	default:
		return 0, errors.New("invalid whence")
	}

	if newOff < 0 || newOff > int64(len(buf)) {
		return 0, errors.New("invalid seek location")
	}
	f.off = newOff
	return newOff, nil
}

// Stat returns the blob's actual size, even if it differs from the size reported during Create.
func (f *File) Size() int64 {
	buf, evicted := f.getData()
	if evicted {
		// TODO - consider whether this is ok or whether we need to store the user-provided blob size in [*File].
		return 0
	}
	return int64(len(buf))
}

// WriteAt implements [io.WriterAt]. It is fully thread-safe.
func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}

	f.sliceMu.Lock()
	defer f.sliceMu.Unlock()

	buf, evicted := f.getData()
	if evicted {
		return 0, ErrEvicted
	}

	end := int(off) + len(p)
	buf, resized := resizeSliceIfNecessary(buf, end)
	n = copy(buf[off:], p)
	if resized {
		f.data.Store(&buf)
	}
	return n, nil
}

func resizeSliceIfNecessary(buf []byte, end int) ([]byte, bool) {
	resized := false
	if len(buf) < end {
		if cap(buf) < end {
			newBuf := make([]byte, end)
			copy(newBuf, buf)
			buf = newBuf
		}
		buf = buf[:end]
		resized = true
	}
	return buf, resized
}

// Write implements io.Writer.
func (f *File) Write(p []byte) (n int, err error) {
	f.sliceMu.Lock() // We need to lock in case we update the pointer to `data`.
	defer f.sliceMu.Unlock()

	buf, evicted := f.getData()
	if evicted {
		return 0, ErrEvicted
	}

	end := int(f.off) + len(p)
	buf, resized := resizeSliceIfNecessary(buf, end)

	n = copy(buf[f.off:], p)
	if resized {
		f.data.Store(&buf)
	}
	f.off += int64(n)
	return n, nil
}

// Off returns the offset for the next Read or Write operation.
// Thread-safe with other File APIs ONLY after File's blob is evicted.
func (f *File) Off() int64 {
	return f.off
}

func (f *File) Cancel() error { return nil } // no-op
func (f *File) Close() error  { return nil } // no-op
func (f *File) Commit() error { return nil } // no-op
