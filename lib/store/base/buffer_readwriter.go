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

	"github.com/aws/aws-sdk-go/aws"
)

var _ FileReadWriter = &BufferReadWriter{}

// BufferReadWriter implements FileReadWriter interface for in-memory buffering.
type BufferReadWriter struct {
	buf    *aws.WriteAtBuffer
	offset int64
}

// NewBufferReadWriter creates a new BufferReadWriter with an initial capacity of size bytes.
func NewBufferReadWriter(size uint64) *BufferReadWriter {
	bytesSlice := make([]byte, 0, size)
	buf := aws.NewWriteAtBuffer(bytesSlice)
	// Although this is default, this is explicitly set to notify that we are reserving
	// only as much capacity as needed
	buf.GrowthCoeff = 1

	return &BufferReadWriter{
		buf:    buf,
		offset: 0,
	}
}

// Write implements io.Writer by using WriteAt with current write offset.
func (b *BufferReadWriter) Write(p []byte) (n int, err error) {
	n, err = b.buf.WriteAt(p, b.offset)
	b.offset += int64(n)
	return n, err
}

// WriteAt implements io.WriterAt for parallel writes.
func (b *BufferReadWriter) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	return b.buf.WriteAt(p, off)
}

// Read implements io.Reader for sequential reads.
func (b *BufferReadWriter) Read(p []byte) (n int, err error) {
	bufBytes := b.buf.Bytes()
	if b.offset >= int64(len(bufBytes)) {
		return 0, io.EOF
	}
	n = copy(p, bufBytes[b.offset:])
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
	bufBytes := b.buf.Bytes()
	if off >= int64(len(bufBytes)) {
		return 0, io.EOF
	}
	n = copy(p, bufBytes[off:])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

// Seek implements io.Seeker.
func (b *BufferReadWriter) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	bufSize := int64(len(b.buf.Bytes()))

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = b.offset + offset
	case io.SeekEnd:
		newOffset = bufSize + offset
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

// Size returns the size of the buffer
func (b *BufferReadWriter) Size() int64 {
	return int64(len(b.buf.Bytes()))
}

// Cancel is no-op
func (b *BufferReadWriter) Cancel() error {
	return nil
}

// Commit is no-op
func (b *BufferReadWriter) Commit() error {
	return nil
}

// Bytes returns the full buffer
func (b *BufferReadWriter) Bytes() []byte {
	return b.buf.Bytes()
}
