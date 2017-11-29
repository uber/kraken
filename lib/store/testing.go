package store

import (
	"bytes"
	"io/ioutil"
	"os"

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
	f, err := os.OpenFile(name, os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}

	return &MockFileReadWriter{File: f}, cleanup.Run
}

type mockGetDownloadFileReadWriterStore struct {
	FileStore
	f FileReadWriter
}

func (s *mockGetDownloadFileReadWriterStore) GetDownloadFileReadWriter(name string) (FileReadWriter, error) {
	return s.f, nil
}

// MockGetDownloadFileReadWriter returns a FileStore wrapping internalFS which overrides
// the GetDownloadFileReadWriter method to return f.
func MockGetDownloadFileReadWriter(internalFS FileStore, f FileReadWriter) FileStore {
	return &mockGetDownloadFileReadWriterStore{internalFS, f}
}

type testFileReader struct {
	*bytes.Reader
}

func (b *testFileReader) Close() error {
	return nil
}

// TestFileReader returns an in-memory FileReader backed by b.
func TestFileReader(b []byte) FileReader {
	return &testFileReader{bytes.NewReader(b)}
}

type testFileReaderCloner struct {
	b []byte
}

func (c *testFileReaderCloner) Clone() (FileReader, error) {
	return TestFileReader(c.b), nil
}

// TestFileReaderCloner returns a Cloner which returns in-memory FileReaders backed by b.
func TestFileReaderCloner(b []byte) FileReaderCloner {
	return &testFileReaderCloner{b}
}
