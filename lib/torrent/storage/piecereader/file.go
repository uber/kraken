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
package piecereader

import (
	"fmt"
	"io"
	"os"

	"github.com/uber/kraken/lib/store"
)

// Opener opens files.
type Opener interface {
	Open() (store.FileReader, error)
}

// FileReader is a storage.PieceReader which reads a piece from a file.
type FileReader struct {
	offset int64
	length int64

	opener Opener
	closer io.Closer
	reader io.Reader
}

// NewFileReader creates a FileReader which reads a piece from f. f should not
// be used once it is given to a FileReader.
func NewFileReader(offset, length int64, opener Opener) *FileReader {
	return &FileReader{
		offset: offset,
		length: length,
		opener: opener,
	}
}

// Read reads a piece in p.
func (r *FileReader) Read(p []byte) (int, error) {
	if r.reader == nil {
		f, err := r.opener.Open()
		if err != nil {
			return 0, fmt.Errorf("open: %s", err)
		}
		if _, err := f.Seek(r.offset, os.SEEK_SET); err != nil {
			return 0, fmt.Errorf("seek: %s", err)
		}
		r.reader = io.LimitReader(f, r.length)
		r.closer = f
	}
	return r.reader.Read(p)
}

// Close closes the underlying file.
func (r *FileReader) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}

// Length returns the length of the piece.
func (r *FileReader) Length() int {
	return int(r.length)
}
