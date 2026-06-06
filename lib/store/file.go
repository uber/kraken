// Copyright (c) 2016-2019 Uber Technologies, Inc.
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
package store

import (
	"os"

	"github.com/uber/kraken/lib/store/base"
)

// FileReadWriter is a readable, writable file.
type FileReadWriter = base.FileReadWriter

// FileReader is a read-only file.
type FileReader = base.FileReader

func newReadWriter(f *os.File, size uint64) FileReadWriter {
	return &rwImpl{
		File: f,
		size: int64(size),
	}
}

var _ FileReadWriter = &rwImpl{}

type rwImpl struct {
	*os.File
	size int64
}

// Size returns the full size of the blob in bytes, even if the blob is not fully written yet.
func (i *rwImpl) Size() int64 {
	return i.size
}

// Cancel is supposed to remove any written content.
// In this implementation file is not actually removed, and it's fine since there won't be name
// collision between upload files.
func (i *rwImpl) Cancel() error {
	return i.Close()
}

// Commit is supposed to flush all content for buffered writer.
// In this implementation all writes write to the file directly through syscall.
func (i *rwImpl) Commit() error {
	return i.Close()
}
