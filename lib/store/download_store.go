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
	"fmt"
	"os"

	"github.com/uber/kraken/lib/store/base"
	"github.com/uber/kraken/lib/store/metadata"
)

// downloadStore provides basic download file operations for content that is
// still being lazily fetched. Intended to be embedded in a higher level
// struct. Shares its backend with cacheStore (see CAStore) so promoting a
// completed download to the cache is an atomic same-volume rename, not a
// copy.
type downloadStore struct {
	state   base.FileState
	backend base.FileStore

	readPartSize  int
	writePartSize int
}

func newDownloadStore(
	dir string, backend base.FileStore, readPartSize, writePartSize int) (*downloadStore, error) {

	if err := os.MkdirAll(dir, 0775); err != nil {
		return nil, fmt.Errorf("mkdir: %s", err)
	}
	state := base.NewFileState(dir)
	return &downloadStore{state, backend, readPartSize, writePartSize}, nil
}

func (s *downloadStore) newFileOp() base.FileOp {
	return s.backend.NewFileOp().AcceptState(s.state)
}

func (s *downloadStore) CreateDownloadFile(name string, length int64) error {
	return s.newFileOp().CreateFile(name, s.state, length)
}

func (s *downloadStore) GetDownloadFileReadWriter(name string) (FileReadWriter, error) {
	return s.newFileOp().GetFileReadWriter(name, s.readPartSize, s.writePartSize)
}

func (s *downloadStore) GetDownloadFileReader(name string) (FileReader, error) {
	return s.newFileOp().GetFileReader(name, s.readPartSize)
}

func (s *downloadStore) GetDownloadFileStat(name string) (os.FileInfo, error) {
	return s.newFileOp().GetFileStat(name)
}

func (s *downloadStore) ListDownloadFiles() ([]string, error) {
	return s.newFileOp().ListNames()
}

func (s *downloadStore) GetOrSetDownloadFileMetadata(name string, md metadata.Metadata) error {
	return s.newFileOp().GetOrSetFileMetadata(name, md)
}

func (s *downloadStore) SetDownloadFileMetadataAt(
	name string, md metadata.Metadata, b []byte, offset int64) (updated bool, err error) {

	return s.newFileOp().SetFileMetadataAt(name, md, b, offset)
}

func (s *downloadStore) DeleteDownloadFile(name string) error {
	return s.newFileOp().DeleteFile(name)
}
