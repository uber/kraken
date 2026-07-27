package memory

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	storelib "github.com/uber/kraken/lib/store"
	"go.uber.org/zap"
)

var _ storelib.FileReadWriter = &handle{}

// handle is the struct returned to clients when they create/open a blob.
// As soon as the blob's data is evicted, the handle no longer has a reference to it,
// ensuring GC can clean it.
type handle struct {
	data    *atomic.Pointer[[]byte]
	sliceMu *sync.RWMutex
	off     int64
	log     *zap.SugaredLogger
}

func newHandle(data *atomic.Pointer[[]byte], sliceMu *sync.RWMutex, log *zap.SugaredLogger) *handle {
	return &handle{
		data:    data,
		sliceMu: sliceMu,
		off:     0,
		log:     log,
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
	if off < 0 {
		return 0, errors.New("negative offset")
	}
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

// Size returns the size of the blob reported by the store user in Create.
func (h *handle) Size() int64 {
	buf, evicted := h.getData()
	if evicted {
		// TODO - consider whether this is ok or whether we need to store the user-provided blob size in [*handle].
		return 0
	}
	return int64(len(buf))
}

// Write implements io.Writer. Not thread-safe, unlike WriteAt.
func (h *handle) Write(p []byte) (n int, err error) {
	buf, evicted := h.getData()
	if evicted {
		return 0, ErrEvicted
	}

	end := int(h.off) + len(p)
	if len(buf) < end {
		h.resizeBuffer(end)
	}

	h.sliceMu.RLock()
	defer h.sliceMu.RUnlock()
	// Reload buf, as the slice might have been mutated.
	buf, evicted = h.getData()
	if evicted {
		h.log.Warn("Client misuse of memory.Store - clients are writing to blobs as they are getting deleted/evicted")
		return 0, ErrEvicted
	}

	n = copy(buf[h.off:], p)
	h.off += int64(n)
	return n, nil
}

// While the buffer is initialized with the client-provided size of the blob, rarely clients underreport blob sizes by a bit.
// In such cases, we need to resize the buffer.
func (h *handle) resizeBuffer(newSize int) {
	h.sliceMu.Lock()
	defer h.sliceMu.Unlock()

	buf, evicted := h.getData()
	if evicted {
		h.log.Warn("Client misuse of memory.Store - clients are writing to blobs as they are getting deleted/evicted")
	}
	if len(buf) >= newSize {
		// Another goroutine already resized and beat us to the race, we don't need to resize anymore.
		return
	}

	newBuf := make([]byte, newSize)
	copy(newBuf, buf)
	h.data.Store(&newBuf)
}

// WriteAt implements [io.WriterAt]. It is fully thread-safe.
func (h *handle) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	buf, evicted := h.getData()
	if evicted {
		return 0, ErrEvicted
	}

	end := int(off) + len(p)
	if len(buf) < end {
		h.resizeBuffer(end)
	}

	h.sliceMu.RLock()
	defer h.sliceMu.RUnlock()
	// Reload buf, as the slice might have been mutated.
	buf, evicted = h.getData()
	if evicted {
		h.log.Warn("Client misuse of memory.Store - clients are writing to blobs as they are getting deleted/evicted")
		return 0, ErrEvicted
	}

	n = copy(buf[off:], p)
	return n, nil
}

func (h *handle) Cancel() error { return nil } // no-op
func (h *handle) Close() error  { return nil } // no-op
func (h *handle) Commit() error { return nil } // no-op
