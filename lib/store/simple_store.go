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
	"io"
	"os"

	"github.com/andres-erbsen/clock"
	"github.com/docker/distribution/uuid"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/lib/store/base"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/log"
)

// SimpleStore allows uploading / caching raw files of any format.
type SimpleStore struct {
	*uploadStore
	*cacheStore
	cleanup *cleanupManager
}

// NewSimpleStore creates a new SimpleStore.
func NewSimpleStore(config SimpleStoreConfig, stats tally.Scope) (*SimpleStore, error) {
	stats = stats.Tagged(map[string]string{
		"module": "simplestore",
	})

	uploadStore, err := newUploadStore(config.UploadDir, config.ReadPartSize, config.WritePartSize)
	if err != nil {
		return nil, fmt.Errorf("new upload store: %s", err)
	}

	cacheBackend := base.NewLocalFileStore(clock.New())
	cacheStore, err := newCacheStore(config.CacheDir, cacheBackend, config.ReadPartSize)
	if err != nil {
		return nil, fmt.Errorf("new cache store: %s", err)
	}

	cleanup, err := newCleanupManager(clock.New(), stats)
	if err != nil {
		return nil, fmt.Errorf("new cleanup manager: %s", err)
	}
	cleanup.addJob("upload", config.UploadCleanup, uploadStore.newFileOp())
	cleanup.addJob("cache", config.CacheCleanup, cacheStore.newFileOp())

	return &SimpleStore{uploadStore, cacheStore, cleanup}, nil
}

// Close terminates goroutines started by s.
func (s *SimpleStore) Close() {
	s.cleanup.stop()
}

// MoveUploadFileToCache commits uploadName as cacheName.
func (s *SimpleStore) MoveUploadFileToCache(uploadName, cacheName string) error {
	uploadPath, err := s.uploadStore.newFileOp().GetFilePath(uploadName)
	if err != nil {
		return err
	}
	defer func() {
		if err := s.DeleteUploadFile(uploadName); err != nil {
			log.With("upload_name", uploadName).Debugf("Failed to delete upload file: %s", err)
		}
	}()
	return s.cacheStore.newFileOp().MoveFileFrom(cacheName, s.cacheStore.state, uploadPath)
}

// CreateCacheFile initializes a cache file for name from r.
func (s *SimpleStore) CreateCacheFile(name string, r io.Reader) error {
	tmp := fmt.Sprintf("%s.%s", name, uuid.Generate().String())
	if err := s.CreateUploadFile(tmp, 0); err != nil {
		return fmt.Errorf("create upload file: %s", err)
	}
	defer func() {
		if err := s.DeleteUploadFile(tmp); err != nil {
			log.With("upload_name", tmp).Debugf("Failed to delete upload file: %s", err)
		}
	}()

	w, err := s.GetUploadFileReadWriter(tmp)
	if err != nil {
		return fmt.Errorf("get upload writer: %s", err)
	}
	defer closers.Close(w)

	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("copy: %s", err)
	}

	if err := s.MoveUploadFileToCache(tmp, name); err != nil && !os.IsExist(err) {
		return fmt.Errorf("move upload file to cache: %s", err)
	}
	return nil
}
