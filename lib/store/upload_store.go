package store

import (
	"fmt"
	"os"

	"github.com/uber/kraken/lib/store/base"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/andres-erbsen/clock"
)

// uploadStore provides basic upload file operations. Intended to be embedded
// in a higher level struct.
type uploadStore struct {
	state   base.FileState
	backend base.FileStore
}

func newUploadStore(dir string) (*uploadStore, error) {
	// Always wipe upload directory on startup.
	os.RemoveAll(dir)

	if err := os.MkdirAll(dir, 0775); err != nil {
		return nil, fmt.Errorf("mkdir: %s", err)
	}
	state := base.NewFileState(dir)
	backend := base.NewLocalFileStore(clock.New())
	return &uploadStore{state, backend}, nil
}

func (s *uploadStore) CreateUploadFile(name string, length int64) error {
	return s.backend.NewFileOp().CreateFile(name, s.state, length)
}

func (s *uploadStore) GetUploadFileStat(name string) (os.FileInfo, error) {
	return s.newFileOp().GetFileStat(name)
}

func (s *uploadStore) GetUploadFileReader(name string) (FileReader, error) {
	return s.newFileOp().GetFileReader(name)
}

func (s *uploadStore) GetUploadFileReadWriter(name string) (FileReadWriter, error) {
	return s.newFileOp().GetFileReadWriter(name)
}

func (s *uploadStore) GetUploadFileMetadata(name string, md metadata.Metadata) error {
	return s.newFileOp().GetFileMetadata(name, md)
}

func (s *uploadStore) SetUploadFileMetadata(name string, md metadata.Metadata) error {
	_, err := s.newFileOp().SetFileMetadata(name, md)
	return err
}

func (s *uploadStore) RangeUploadMetadata(name string, f func(metadata.Metadata) error) error {
	return s.newFileOp().RangeFileMetadata(name, f)
}

func (s *uploadStore) DeleteUploadFile(name string) error {
	return s.newFileOp().DeleteFile(name)
}

func (s *uploadStore) newFileOp() base.FileOp {
	return s.backend.NewFileOp().AcceptState(s.state)
}
