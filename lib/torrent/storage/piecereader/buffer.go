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

import "bytes"

// Buffer is a storage.PieceReader which reads a piece from an in-memory buffer.
type Buffer struct {
	reader *bytes.Reader
}

// NewBuffer returns a new Buffer for b.
func NewBuffer(b []byte) *Buffer {
	return &Buffer{bytes.NewReader(b)}
}

// Read reads a piece into p.
func (b *Buffer) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

// Close noops.
func (b *Buffer) Close() error {
	return nil
}

// Length returns the length of the piece.
func (b *Buffer) Length() int {
	return b.reader.Len()
}
