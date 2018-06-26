package store

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/testutil"
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
