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
	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
)

// CADownloadStore allows simultaneously downloading and uploading
// content-adddressable files.
type CADownloadStore struct {
	backend       base.FileStore
	downloadState base.FileState
	cacheState    base.FileState
	cleanup       *cleanupManager
}

// NewCADownloadStore creates a new CADownloadStore.
func NewCADownloadStore(config CADownloadStoreConfig, stats tally.Scope) (*CADownloadStore, error) {
	stats = stats.Tagged(map[string]string{
		"module": "cadownloadstore",
	})

	for _, dir := range []string{config.DownloadDir, config.CacheDir} {
		if err := os.MkdirAll(dir, 0775); err != nil {
			return nil, fmt.Errorf("mkdir %s: %s", dir, err)
		}
	}

	backend := base.NewCASFileStore(clock.New())
	downloadState := base.NewFileState(config.DownloadDir)
	cacheState := base.NewFileState(config.CacheDir)

	cleanup, err := newCleanupManager(clock.New(), stats)
	if err != nil {
		return nil, fmt.Errorf("new cleanup manager: %s", err)
	}
	cleanup.addJob(
		"download",
		config.DownloadCleanup,
		backend.NewFileOp().AcceptState(downloadState))
	cleanup.addJob(
		"cache",
		config.CacheCleanup,
		backend.NewFileOp().AcceptState(cacheState))

	return &CADownloadStore{
		backend:       backend,
		downloadState: downloadState,
		cacheState:    cacheState,
		cleanup:       cleanup,
	}, nil
}

// Close terminates all goroutines started by s.
func (s *CADownloadStore) Close() {
	s.cleanup.stop()
}

// CreateDownloadFile creates an empty download file initialized with length.
func (s *CADownloadStore) CreateDownloadFile(name string, length int64) error {
	return s.backend.NewFileOp().CreateFile(name, s.downloadState, length)
}

// GetDownloadFileReadWriter returns a FileReadWriter for name.
func (s *CADownloadStore) GetDownloadFileReadWriter(name string) (FileReadWriter, error) {
	return s.backend.NewFileOp().AcceptState(s.downloadState).GetFileReadWriter(name)
}

// MoveDownloadFileToCache moves a download file to the cache.
func (s *CADownloadStore) MoveDownloadFileToCache(name string) error {
	return s.backend.NewFileOp().AcceptState(s.downloadState).MoveFile(name, s.cacheState)
}

// GetCacheFileReader gets a cache file reader. Implemented for compatibility with
// other stores.
func (s *CADownloadStore) GetCacheFileReader(name string) (FileReader, error) {
	return s.Cache().GetFileReader(name)
}

// GetCacheFileStat stats a cache file. Implemented for compatibility with other
// stores.
func (s *CADownloadStore) GetCacheFileStat(name string) (os.FileInfo, error) {
	return s.Cache().GetFileStat(name)
}

// InCacheError returns true for errors originating from file store operations
// which do not accept files in cache state.
func (s *CADownloadStore) InCacheError(err error) bool {
	fse, ok := err.(*base.FileStateError)
	return ok && fse.State == s.cacheState
}

// InDownloadError returns true for errors originating from file store operations
// which do not accept files in download state.
func (s *CADownloadStore) InDownloadError(err error) bool {
	fse, ok := err.(*base.FileStateError)
	return ok && fse.State == s.downloadState
}

// CADownloadStoreScope scopes what states an operation may be accepted within.
// Should only be used for read / write operations which are acceptable in any
// state.
type CADownloadStoreScope struct {
	store *CADownloadStore
	op    base.FileOp
}

func (s *CADownloadStore) states() *CADownloadStoreScope {
	return &CADownloadStoreScope{
		store: s,
		op:    s.backend.NewFileOp(),
	}
}

func (a *CADownloadStoreScope) download() *CADownloadStoreScope {
	a.op = a.op.AcceptState(a.store.downloadState)
	return a
}

func (a *CADownloadStoreScope) cache() *CADownloadStoreScope {
	a.op = a.op.AcceptState(a.store.cacheState)
	return a
}

// Download scopes the store to files in the download state.
func (s *CADownloadStore) Download() *CADownloadStoreScope {
	return s.states().download()
}

// Cache scopes the store to files in the cache state.
func (s *CADownloadStore) Cache() *CADownloadStoreScope {
	return s.states().cache()
}

// Any scopes the store to files in any state.
func (s *CADownloadStore) Any() *CADownloadStoreScope {
	return s.states().download().cache()
}

// GetFileReader returns a reader for name.
func (a *CADownloadStoreScope) GetFileReader(name string) (FileReader, error) {
	return a.op.GetFileReader(name)
}

// GetFileStat returns file info for name.
func (a *CADownloadStoreScope) GetFileStat(name string) (os.FileInfo, error) {
	return a.op.GetFileStat(name)
}

// DeleteFile deletes name.
func (a *CADownloadStoreScope) DeleteFile(name string) error {
	return a.op.DeleteFile(name)
}

// GetMetadata returns the metadata content of md for name.
func (a *CADownloadStoreScope) GetMetadata(name string, md metadata.Metadata) error {
	return a.op.GetFileMetadata(name, md)
}

// SetMetadata writes b to metadata content of md for name.
func (a *CADownloadStoreScope) SetMetadata(
	name string, md metadata.Metadata) (updated bool, err error) {

	return a.op.SetFileMetadata(name, md)
}

// SetMetadataAt writes b to metadata content of md starting at index i for name.
func (a *CADownloadStoreScope) SetMetadataAt(
	name string, md metadata.Metadata, b []byte, offset int64) (updated bool, err error) {

	return a.op.SetFileMetadataAt(name, md, b, offset)
}

// GetOrSetMetadata returns the metadata content of md for name, or
// initializes the metadata content to b if not set.
func (a *CADownloadStoreScope) GetOrSetMetadata(name string, md metadata.Metadata) error {
	return a.op.GetOrSetFileMetadata(name, md)
}
