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
	"fmt"
	"os"

	"github.com/uber/kraken/lib/store/base"
	"github.com/uber/kraken/lib/store/metadata"
)

// cacheStore provides basic cache file operations. Intended to be embedded in
// higher level structs.
type cacheStore struct {
	state   base.FileState
	backend base.FileStore
}

func newCacheStore(dir string, backend base.FileStore) (*cacheStore, error) {
	if err := os.MkdirAll(dir, 0775); err != nil {
		return nil, fmt.Errorf("mkdir: %s", err)
	}
	state := base.NewFileState(dir)
	return &cacheStore{state, backend}, nil
}

func (s *cacheStore) GetCacheFileReader(name string) (FileReader, error) {
	return s.newFileOp().GetFileReader(name)
}

func (s *cacheStore) GetCacheFileStat(name string) (os.FileInfo, error) {
	return s.newFileOp().GetFileStat(name)
}

func (s *cacheStore) DeleteCacheFile(name string) error {
	return s.newFileOp().DeleteFile(name)
}

func (s *cacheStore) GetCacheFileMetadata(name string, md metadata.Metadata) error {
	return s.newFileOp().GetFileMetadata(name, md)
}

func (s *cacheStore) SetCacheFileMetadata(name string, md metadata.Metadata) (bool, error) {
	return s.newFileOp().SetFileMetadata(name, md)
}

func (s *cacheStore) GetOrSetCacheFileMetadata(name string, md metadata.Metadata) error {
	return s.newFileOp().GetOrSetFileMetadata(name, md)
}

func (s *cacheStore) DeleteCacheFileMetadata(name string, md metadata.Metadata) error {
	return s.newFileOp().DeleteFileMetadata(name, md)
}

func (s *cacheStore) ListCacheFiles() ([]string, error) {
	return s.newFileOp().ListNames()
}

func (s *cacheStore) newFileOp() base.FileOp {
	return s.backend.NewFileOp().AcceptState(s.state)
}
