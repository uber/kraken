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
package transfer

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/namepath"
	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/utils/closers"
)

type testTransferer struct {
	tagPather namepath.Pather
	tags      map[string]core.Digest
	store     *disk.Store
}

// NewTestTransferer creates a Transferer which stores blobs in store and tags
// in memory for testing purposes.
func NewTestTransferer(store *disk.Store) ImageTransferer {
	tagPather, err := namepath.New("", namepath.DockerTag)
	if err != nil {
		panic(err)
	}
	return &testTransferer{
		tagPather: tagPather,
		tags:      make(map[string]core.Digest),
		store:     store,
	}
}

// Stat returns blob info from local cache.
func (t *testTransferer) Stat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	fi, err := t.store.ScopeComplete().Stat(d.Hex())
	if err != nil {
		return nil, fmt.Errorf("stat cache file: %w", err)
	}
	return core.NewBlobInfo(fi.Size()), nil
}

func (t *testTransferer) Download(namespace string, d core.Digest) (storelib.FileReader, error) {
	return t.store.ScopeComplete().Open(d.Hex())
}

// Upload caches blob under d, tolerating re-uploads of an already-cached
// digest (content-addressed, so a collision is always the same bytes).
func (t *testTransferer) Upload(namespace string, d core.Digest, blob storelib.FileReader) error {
	f, err := t.store.Create(d.Hex(), 1)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return fmt.Errorf("create cache file: %w", err)
	}
	_, err = io.Copy(f, blob)
	closers.Close(f)
	if err != nil {
		return fmt.Errorf("write content: %w", err)
	}
	return t.store.MarkComplete(d.Hex())
}

func (t *testTransferer) GetTag(tag string) (core.Digest, error) {
	p, err := t.tagPather.BlobPath(tag)
	if err != nil {
		return core.Digest{}, err
	}
	d, ok := t.tags[p]
	if !ok {
		return core.Digest{}, ErrTagNotFound
	}
	return d, nil
}

func (t *testTransferer) PutTag(tag string, d core.Digest) error {
	p, err := t.tagPather.BlobPath(tag)
	if err != nil {
		return err
	}
	t.tags[p] = d
	return nil
}

func (t *testTransferer) ListTags(prefix string) ([]string, error) {
	prefix = path.Join(t.tagPather.BasePath(), prefix)
	var tags []string
	for path := range t.tags {
		if strings.HasPrefix(path, prefix) {
			tag, err := t.tagPather.NameFromBlobPath(path)
			if err != nil {
				return nil, fmt.Errorf("invalid tag path %s: %s", path, err)
			}
			tags = append(tags, tag)
		}
	}
	return tags, nil
}
