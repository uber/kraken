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
package store

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/testutil"
)

// MockFileReadWriter is a mock FileReadWriter that is backed by a
// physical file. This is preferred to a gomock struct because read/write
// operations are greatly simplified.
type MockFileReadWriter struct {
	*os.File
	Committed bool
}

// Commit implements FileReadWriter.Commit
func (f *MockFileReadWriter) Commit() error { panic("commit not implemented") }

// Cancel implements FileReadWriter.Cancel
func (f *MockFileReadWriter) Cancel() error { panic("cancel not implemented") }

// Size implements FileReadWriter.Size
func (f *MockFileReadWriter) Size() int64 { panic("size not implemented") }

var _ FileReadWriter = (*MockFileReadWriter)(nil)

// NewMockFileReadWriter returns a new MockFileReadWriter and a cleanup function.
func NewMockFileReadWriter(content []byte) (*MockFileReadWriter, func()) {
	cleanup := new(testutil.Cleanup)
	defer cleanup.Recover()

	tmp, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}
	name := tmp.Name()
	cleanup.Add(func() { os.Remove(name) })

	if _, err := tmp.Write(content); err != nil {
		panic(err)
	}
	if err := tmp.Close(); err != nil {
		panic(err)
	}

	// Open fresh file.
	f, err := os.OpenFile(name, os.O_RDWR, 0775)
	if err != nil {
		panic(err)
	}

	return &MockFileReadWriter{File: f}, cleanup.Run
}

// RunDownload downloads content to cads.
func RunDownload(cads *CADownloadStore, d core.Digest, content []byte) error {
	if err := cads.CreateDownloadFile(d.Hex(), int64(len(content))); err != nil {
		return err
	}
	w, err := cads.GetDownloadFileReadWriter(d.Hex())
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, bytes.NewReader(content)); err != nil {
		return err
	}
	return cads.MoveDownloadFileToCache(d.Hex())
}
