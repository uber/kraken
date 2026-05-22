// Copyright (c) 2016-2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package base

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

var _ FileReadWriter = &BufferReadWriter{}

// BufferReadWriter implements FileReadWriter for in-memory buffering.
//
// Pre-sizing (size > 0) allows concurrent WriteAt calls to non-overlapping
// ranges to run in parallel. Without pre-sizing, each write that grows the
// buffer is serialized.
//
// Bytes, Size, Write, Read, ReadAt, and Seek must not be called concurrently
// with each other or with WriteAt.
type BufferReadWriter struct {
	mu      sync.RWMutex
	buf     []byte
	written atomic.Int64
	offset  int64
}

// NewBufferReadWriter creates a new BufferReadWriter pre-allocated to size bytes.
// Pass the exact blob size when known so concurrent WriteAt calls for
// non-overlapping shard ranges can run in parallel without writer serialization.
func NewBufferReadWriter(size uint64) *BufferReadWriter {
	return &BufferReadWriter{buf: make([]byte, size)}
}

// Write implements io.Writer using the current sequential write offset.
func (b *BufferReadWriter) Write(p []byte) (n int, err error) {
	n, err = b.WriteAt(p, b.offset)
	b.offset += int64(n)
	return n, err
}

// WriteAt implements io.WriterAt.
//
// Fast path (off+len(p) within pre-allocated buffer): multiple goroutines may
// call WriteAt concurrently, provided their byte ranges do not overlap.
// Slow path (write extends beyond current buffer): acquires an exclusive lock
// to grow the buffer, then writes.
func (b *BufferReadWriter) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	end := off + int64(len(p))
	if end < off {
		return 0, fmt.Errorf("write at offset %d length %d overflows int64", off, len(p))
	}

	// fast path
	if n, shouldGrowBuffer := b.updateBuffer(p, off, end); !shouldGrowBuffer {
		return n, nil
	}

	// slow path
	b.mu.Lock()
	defer b.mu.Unlock()
	if end > int64(len(b.buf)) {
		grown := make([]byte, end)
		copy(grown, b.buf)
		b.buf = grown
	}
	n := copy(b.buf[off:], p)
	if end > b.written.Load() {
		b.written.Store(end)
	}
	return n, nil
}

// updateBuffer copies p into [off, end).
// Returns (-1, true) if end exceeds the buffer, causing WriteAt to take the slow path.
func (b *BufferReadWriter) updateBuffer(p []byte, off, end int64) (int, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if end <= int64(len(b.buf)) {
		n := copy(b.buf[off:], p)
		for {
			cur := b.written.Load()
			if end <= cur || b.written.CompareAndSwap(cur, end) {
				break
			}
		}
		return n, false
	}
	return -1, true
}

// Read implements io.Reader for sequential reads.
func (b *BufferReadWriter) Read(p []byte) (n int, err error) {
	written := b.written.Load()
	if b.offset >= written {
		return 0, io.EOF
	}
	n = copy(p, b.buf[b.offset:written])
	b.offset += int64(n)
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

// ReadAt implements io.ReaderAt.
func (b *BufferReadWriter) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	b.mu.RLock()
	buf := b.buf
	written := b.written.Load()
	b.mu.RUnlock()
	if off >= written {
		return 0, io.EOF
	}
	n = copy(p, buf[off:written])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

// Seek implements io.Seeker.
func (b *BufferReadWriter) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = b.offset + offset
	case io.SeekEnd:
		newOffset = b.written.Load() + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	if newOffset < 0 {
		return 0, fmt.Errorf("negative position: %d", newOffset)
	}
	b.offset = newOffset
	return newOffset, nil
}

// Close is no-op
func (b *BufferReadWriter) Close() error {
	return nil
}

// Size returns the largest end offset written so far.
func (b *BufferReadWriter) Size() int64 { return b.written.Load() }

// Cancel is no-op
func (b *BufferReadWriter) Cancel() error {
	return nil
}

// Commit is no-op
func (b *BufferReadWriter) Commit() error {
	return nil
}

// Bytes returns the buffer up to the highest offset written so far.
// Any gaps between writes are zero-filled.
func (b *BufferReadWriter) Bytes() []byte {
	return b.buf[:b.written.Load()]
}
