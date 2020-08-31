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
	"hash"
	"io"
	"os"
	"path"

	"github.com/andres-erbsen/clock"
	"github.com/docker/distribution/uuid"
	"github.com/spaolacci/murmur3"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hrw"
	"github.com/uber/kraken/lib/store/base"
)

// CAStore allows uploading / caching content-addressable files.
type CAStore struct {
	config CAStoreConfig

	*uploadStore
	*cacheStore
	cleanup *cleanupManager
}

// NewCAStore creates a new CAStore.
func NewCAStore(config CAStoreConfig, stats tally.Scope) (*CAStore, error) {
	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "castore",
	})

	uploadStore, err := newUploadStore(config.UploadDir)
	if err != nil {
		return nil, fmt.Errorf("new upload store: %s", err)
	}

	cacheBackend := base.NewCASFileStoreWithLRUMap(config.Capacity, clock.New())
	cacheStore, err := newCacheStore(config.CacheDir, cacheBackend)
	if err != nil {
		return nil, fmt.Errorf("new cache store: %s", err)
	}

	if err := initCASVolumes(config.CacheDir, config.Volumes); err != nil {
		return nil, fmt.Errorf("init cas volumes: %s", err)
	}

	cleanup, err := newCleanupManager(clock.New(), stats)
	if err != nil {
		return nil, fmt.Errorf("new cleanup manager: %s", err)
	}
	cleanup.addJob("upload", config.UploadCleanup, uploadStore.newFileOp())
	cleanup.addJob("cache", config.CacheCleanup, cacheStore.newFileOp())

	return &CAStore{config, uploadStore, cacheStore, cleanup}, nil
}

// Close terminates any goroutines started by s.
func (s *CAStore) Close() {
	s.cleanup.stop()
}

// MoveUploadFileToCache commits uploadName as cacheName. Clients are expected
// to validate the content of the upload file matches the cacheName digest.
func (s *CAStore) MoveUploadFileToCache(uploadName, cacheName string) error {
	uploadPath, err := s.uploadStore.newFileOp().GetFilePath(uploadName)
	if err != nil {
		return err
	}
	defer s.DeleteUploadFile(uploadName)

	f, err := s.uploadStore.newFileOp().GetFileReader(uploadName)
	if err != nil {
		return fmt.Errorf("get file reader %s: %s", uploadName, err)
	}
	defer f.Close()
	if err := s.verify(f, cacheName); err != nil {
		return fmt.Errorf("verify digest: %s", err)
	}

	return s.cacheStore.newFileOp().MoveFileFrom(cacheName, s.cacheStore.state, uploadPath)
}

// CreateCacheFile initializes a cache file for name from r. name should be a raw
// hex sha256 digest, and the contents of r must hash to name.
func (s *CAStore) CreateCacheFile(name string, r io.Reader) error {
	return s.WriteCacheFile(name, func(w FileReadWriter) error {
		_, err := io.Copy(w, r)
		return err
	})
}

// WriteCacheFile initializes a cache file for name by passing a temporary
// upload file writer to the write function.
func (s *CAStore) WriteCacheFile(name string, write func(w FileReadWriter) error) error {
	tmp := fmt.Sprintf("%s.%s", name, uuid.Generate().String())
	if err := s.CreateUploadFile(tmp, 0); err != nil {
		return fmt.Errorf("create upload file: %s", err)
	}
	defer s.DeleteUploadFile(tmp)

	w, err := s.GetUploadFileReadWriter(tmp)
	if err != nil {
		return fmt.Errorf("get upload writer: %s", err)
	}
	defer w.Close()

	if err := write(w); err != nil {
		return err
	}
	if err := s.MoveUploadFileToCache(tmp, name); err != nil && !os.IsExist(err) {
		return fmt.Errorf("move upload file to cache: %s", err)
	}
	return nil
}

// verify verifies that name is a valid SHA256 digest, and checks if the given
// blob content matches the digset unless explicitly skipped.
func (s *CAStore) verify(r io.Reader, name string) error {
	// Verify that expected name is a valid SHA256 digest.
	expected, err := core.NewSHA256DigestFromHex(name)
	if err != nil {
		return fmt.Errorf("new digest from file name: %s", err)
	}

	if !s.config.SkipHashVerification {
		digester := core.NewDigester()
		computed, err := digester.FromReader(r)
		if err != nil {
			return fmt.Errorf("calculate digest: %s", err)
		}
		if computed != expected {
			return fmt.Errorf("computed digest %s doesn't match expected value %s", computed, expected)
		}
	}
	return nil
}

func initCASVolumes(dir string, volumes []Volume) error {
	if len(volumes) == 0 {
		return nil
	}

	rendezvousHash := hrw.NewRendezvousHash(
		func() hash.Hash { return murmur3.New64() },
		hrw.UInt64ToFloat64)

	for _, v := range volumes {
		if _, err := os.Stat(v.Location); err != nil {
			return fmt.Errorf("verify volume: %s", err)
		}
		rendezvousHash.AddNode(v.Location, v.Weight)
	}

	// Create 256 symlinks under dir.
	for subdirIndex := 0; subdirIndex < 256; subdirIndex++ {
		subdirName := fmt.Sprintf("%02X", subdirIndex)
		nodes := rendezvousHash.GetOrderedNodes(subdirName, 1)
		if len(nodes) != 1 {
			return fmt.Errorf("calculate volume for subdir: %s", subdirName)
		}
		sourcePath := path.Join(nodes[0].Label, path.Base(dir), subdirName)
		if err := os.MkdirAll(sourcePath, 0775); err != nil {
			return fmt.Errorf("volume source path: %s", err)
		}
		targetPath := path.Join(dir, subdirName)
		if err := createOrUpdateSymlink(sourcePath, targetPath); err != nil {
			return fmt.Errorf("symlink to volume: %s", err)
		}
	}

	return nil
}
