package memory

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	storelib "github.com/uber/kraken/lib/store"
)

var _ storelib.FileReadWriter = &handle{}

// handle is the struct returned to clients when they create/open a blob.
// As soon as the blob's data is evicted, the handle no longer has a reference to it,
// ensuring GC can clean it.
type handle struct {
	data    *atomic.Pointer[[]byte]
	sliceMu *sync.Mutex // A potential optimization if contention is too high: currently all writes are not parallelized. However, writes that only mutate the array but not the slice are parallelizable with each other. Thus, we could transition to a RWMutex to enable that.
	off     int64
}

func newHandle(data *atomic.Pointer[[]byte], sliceMu *sync.Mutex) *handle {
	return &handle{
		data:    data,
		sliceMu: sliceMu,
		off:     0,
	}
}

func (h *handle) getData() (data []byte, evicted bool) {
	buf := h.data.Load()
	if buf == nil {
		return nil, true
	}
	return *buf, false
}

func (h *handle) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	buf, evicted := h.getData()
	if evicted {
		return 0, ErrEvicted
	}

	if h.off >= int64(len(buf)) {
		return 0, io.EOF
	}
	n = copy(p, buf[h.off:])
	h.off += int64(n)
	return n, nil
}

// ReadAt implements [io.ReaderAt]. Thread-safe.
func (h *handle) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, errors.New("negative offset")
	}

	buf, evicted := h.getData()
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
func (h *handle) Seek(off int64, whence int) (int64, error) {
	buf, evicted := h.getData()
	if evicted {
		return 0, ErrEvicted
	}

	var newOff int64
	switch whence {
	case io.SeekStart:
		newOff = off
	case io.SeekCurrent:
		newOff = h.off + off
	case io.SeekEnd:
		newOff = int64(len(buf)) + off
	default:
		return 0, errors.New("invalid whence")
	}

	if newOff < 0 || newOff > int64(len(buf)) {
		return 0, errors.New("invalid seek location")
	}
	h.off = newOff
	return newOff, nil
}

// Stat returns the blob's actual size, even if it differs from the size reported during Create.
func (h *handle) Size() int64 {
	buf, evicted := h.getData()
	if evicted {
		// TODO - consider whether this is ok or whether we need to store the user-provided blob size in [*handle].
		return 0
	}
	return int64(len(buf))
}

// WriteAt implements [io.WriterAt]. It is fully thread-safe.
func (h *handle) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}

	h.sliceMu.Lock()
	defer h.sliceMu.Unlock()

	buf, evicted := h.getData()
	if evicted {
		return 0, ErrEvicted
	}

	end := int(off) + len(p)
	buf, resized := resizeSliceIfNecessary(buf, end)
	n = copy(buf[off:], p)
	if resized {
		h.data.Store(&buf)
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
func (h *handle) Write(p []byte) (n int, err error) {
	h.sliceMu.Lock() // We need to lock in case we update the pointer to `data`.
	defer h.sliceMu.Unlock()

	buf, evicted := h.getData()
	if evicted {
		return 0, ErrEvicted
	}

	end := int(h.off) + len(p)
	buf, resized := resizeSliceIfNecessary(buf, end)

	n = copy(buf[h.off:], p)
	if resized {
		h.data.Store(&buf)
	}
	h.off += int64(n)
	return n, nil
}

func (h *handle) Cancel() error { return nil } // no-op
func (h *handle) Close() error  { return nil } // no-op
func (h *handle) Commit() error { return nil } // no-op
