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
package tagstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/lib/persistedretry/writeback"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/log"

	"github.com/uber-go/tally"
)

// Store errors.
var (
	ErrTagNotFound = errors.New("tag not found")
)

// FileStore defines operations required for storing tags on disk.
type FileStore interface {
	CreateCacheFile(name string, r io.Reader) error
	SetCacheFileMetadata(name string, md metadata.Metadata) (bool, error)
	GetCacheFileReader(name string) (store.FileReader, error)
}

// Store defines tag storage operations.
type Store interface {
	Put(tag string, d core.Digest, writeBackDelay time.Duration) error
	Get(tag string) (core.Digest, error)
}

// tagStore encapsulates two-level tag storage:
// 1. On-disk file store: persists tags for availability / write-back purposes.
// 2. Remote storage: durable tag storage.
type tagStore struct {
	config           Config
	fs               FileStore
	backends         *backend.Manager
	writeBackManager persistedretry.Manager

	// writeBackStrategy determines how tags are written to backend storage.
	// Set at initialization based on WriteThrough config.
	writeBackStrategy func(task persistedretry.Task) error
}

// New creates a new Store.
func New(
	config Config,
	stats tally.Scope,
	fs FileStore,
	backends *backend.Manager,
	writeBackManager persistedretry.Manager,
) Store {
	stats = stats.Tagged(map[string]string{
		"module": "tagstore",
	})

	s := &tagStore{
		config:           config,
		fs:               fs,
		backends:         backends,
		writeBackManager: writeBackManager,
	}

	// Set write-back strategy based on configuration
	if config.WriteThrough {
		s.writeBackStrategy = s.writeThroughStrategy
	} else {
		s.writeBackStrategy = s.asyncWriteBackStrategy
	}

	return s
}

func (s *tagStore) Put(tag string, d core.Digest, writeBackDelay time.Duration) error {
	if err := s.writeTagToDisk(tag, d); err != nil {
		return fmt.Errorf("write tag to disk: %s", err)
	}
	if _, err := s.fs.SetCacheFileMetadata(tag, metadata.NewPersist(true)); err != nil {
		return fmt.Errorf("set persist metadata: %s", err)
	}

	task := writeback.NewTask(tag, tag, writeBackDelay)
	return s.writeBackStrategy(task)
}

func (s *tagStore) Get(tag string) (d core.Digest, err error) {
	for _, resolve := range []func(tag string) (core.Digest, error){
		s.resolveFromDisk,
		s.resolveFromBackend,
	} {
		d, err = resolve(tag)
		if err == ErrTagNotFound {
			continue
		}
		break
	}
	return d, err
}

// writeThroughStrategy writes tags synchronously to backend storage.
func (s *tagStore) writeThroughStrategy(task persistedretry.Task) error {
	if err := s.writeBackManager.SyncExec(task); err != nil {
		return fmt.Errorf("sync exec write-back task: %s", err)
	}
	return nil
}

// asyncWriteBackStrategy queues tags for asynchronous write-back to backend storage.
func (s *tagStore) asyncWriteBackStrategy(task persistedretry.Task) error {
	if err := s.writeBackManager.Add(task); err != nil {
		return fmt.Errorf("add write-back task: %s", err)
	}
	return nil
}

func (s *tagStore) writeTagToDisk(tag string, d core.Digest) error {
	buf := bytes.NewBufferString(d.String())
	if err := s.fs.CreateCacheFile(tag, buf); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

func (s *tagStore) resolveFromDisk(tag string) (core.Digest, error) {
	log.With("tag", tag).Debug("Attempting to resolve tag from disk cache")

	f, err := s.fs.GetCacheFileReader(tag)
	if err != nil {
		if os.IsNotExist(err) {
			log.With("tag", tag).Debug("Tag not found in disk cache")
			return core.Digest{}, ErrTagNotFound
		}
		log.With("tag", tag).Errorf("Failed to read tag from disk cache: %s", err)
		return core.Digest{}, fmt.Errorf("fs: %s", err)
	}
	defer closers.Close(f)
	var b bytes.Buffer
	if _, err := io.Copy(&b, f); err != nil {
		log.With("tag", tag).Errorf("Failed to copy tag data from disk: %s", err)
		return core.Digest{}, fmt.Errorf("copy from fs: %s", err)
	}
	d, err := core.ParseSHA256Digest(b.String())
	if err != nil {
		log.With("tag", tag).Errorf("Failed to parse digest from disk cache: %s", err)
		return core.Digest{}, fmt.Errorf("parse fs digest: %s", err)
	}

	log.With("tag", tag, "digest", d.String()).Debug("Successfully resolved tag from disk cache")
	return d, nil
}

func (s *tagStore) resolveFromBackend(tag string) (core.Digest, error) {
	log.With("tag", tag).Debug("Attempting to resolve tag from backend")

	backendClient, err := s.backends.GetClient(tag)
	if err != nil {
		log.With("tag", tag).Errorf("Failed to get backend client: %s", err)
		return core.Digest{}, fmt.Errorf("backend manager: %s", err)
	}
	var b bytes.Buffer
	if err := backendClient.Download(tag, tag, &b); err != nil {
		if err == backenderrors.ErrBlobNotFound {
			log.With("tag", tag).Debug("Tag not found in backend")
			return core.Digest{}, ErrTagNotFound
		}
		log.With("tag", tag).Errorf("Failed to download tag from backend: %s", err)
		return core.Digest{}, fmt.Errorf("backend client: %s", err)
	}
	d, err := core.ParseSHA256Digest(b.String())
	if err != nil {
		log.With("tag", tag).Errorf("Failed to parse digest from backend: %s", err)
		return core.Digest{}, fmt.Errorf("parse backend digest: %s", err)
	}

	log.With("tag", tag, "digest", d.String()).Info("Successfully resolved tag from backend")
	return d, nil
}
