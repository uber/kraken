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
}

// New creates a new Store.
func New(
	config Config,
	stats tally.Scope,
	fs FileStore,
	backends *backend.Manager,
	writeBackManager persistedretry.Manager) Store {

	stats = stats.Tagged(map[string]string{
		"module": "tagstore",
	})

	return &tagStore{
		config:           config,
		fs:               fs,
		backends:         backends,
		writeBackManager: writeBackManager,
	}
}

func (s *tagStore) Put(tag string, d core.Digest, writeBackDelay time.Duration) error {
	if err := s.writeTagToDisk(tag, d); err != nil {
		return fmt.Errorf("write tag to disk: %s", err)
	}
	if _, err := s.fs.SetCacheFileMetadata(tag, metadata.NewPersist(true)); err != nil {
		return fmt.Errorf("set persist metadata: %s", err)
	}

	task := writeback.NewTask(tag, tag, writeBackDelay)
	if s.config.WriteThrough {
		if err := s.writeBackManager.SyncExec(task); err != nil {
			return fmt.Errorf("sync exec write-back task: %s", err)
		}
	} else {
		if err := s.writeBackManager.Add(task); err != nil {
			return fmt.Errorf("add write-back task: %s", err)
		}
	}
	return nil
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

func (s *tagStore) writeTagToDisk(tag string, d core.Digest) error {
	buf := bytes.NewBufferString(d.String())
	if err := s.fs.CreateCacheFile(tag, buf); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

func (s *tagStore) resolveFromDisk(tag string) (core.Digest, error) {
	f, err := s.fs.GetCacheFileReader(tag)
	if err != nil {
		if os.IsNotExist(err) {
			return core.Digest{}, ErrTagNotFound
		}
		return core.Digest{}, fmt.Errorf("fs: %s", err)
	}
	defer f.Close()
	var b bytes.Buffer
	if _, err := io.Copy(&b, f); err != nil {
		return core.Digest{}, fmt.Errorf("copy from fs: %s", err)
	}
	d, err := core.ParseSHA256Digest(b.String())
	if err != nil {
		return core.Digest{}, fmt.Errorf("parse fs digest: %s", err)
	}
	return d, nil
}

func (s *tagStore) resolveFromBackend(tag string) (core.Digest, error) {
	backendClient, err := s.backends.GetClient(tag)
	if err != nil {
		return core.Digest{}, fmt.Errorf("backend manager: %s", err)
	}
	var b bytes.Buffer
	if err := backendClient.Download(tag, tag, &b); err != nil {
		if err == backenderrors.ErrBlobNotFound {
			return core.Digest{}, ErrTagNotFound
		}
		return core.Digest{}, fmt.Errorf("backend client: %s", err)
	}
	d, err := core.ParseSHA256Digest(b.String())
	if err != nil {
		return core.Digest{}, fmt.Errorf("parse backend digest: %s", err)
	}
	return d, nil
}
