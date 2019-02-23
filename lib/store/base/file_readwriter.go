// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package base

import (
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

	Cancel() error // required by docker registry.
	Commit() error // required by docker registry.
}

// LocalFileReadWriter implements FileReadWriter interface, provides read/write
// operation on a local file.
type localFileReadWriter struct {
	entry      *localFileEntry
	descriptor *os.File
}

func (readWriter *localFileReadWriter) close() error {
	return readWriter.descriptor.Close()
}

// Close closes underlying OS.File object.
func (readWriter localFileReadWriter) Close() error {
	return readWriter.close()
}

// Write writes up to len(b) bytes to the File.
func (readWriter localFileReadWriter) Write(p []byte) (int, error) {
	return readWriter.descriptor.Write(p)
}

// WriteAt writes len(p) bytes from p to the underlying data stream at offset.
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

// Seek sets the offset for the next Read or Write on file to offset,
// interpreted according to whence:
// 0 means relative to the origin of the file;
// 1 means relative to the current offset;
// 2 means relative to the end.
func (readWriter localFileReadWriter) Seek(offset int64, whence int) (int64, error) {
	return readWriter.descriptor.Seek(offset, whence)
}

// Size returns the size of the file.
func (readWriter localFileReadWriter) Size() int64 {
	// Use file entry instead of descriptor, because descriptor could have been closed.
	fileInfo, err := readWriter.entry.GetStat()

	if err != nil {
		return 0
	}
	return fileInfo.Size()
}

// Cancel is supposed to remove any written content.
// In this implementation file is not actually removed, and it's fine since there won't be name
// collision between upload files.
func (readWriter localFileReadWriter) Cancel() error {
	return readWriter.close()
}

// Commit is supposed to flush all content for buffered writer.
// In this implementation all writes write to the file directly through syscall.
func (readWriter localFileReadWriter) Commit() error {
	return readWriter.close()
}
