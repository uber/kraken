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
	"os"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/scheduler"
)

var _ ImageTransferer = (*ReadOnlyTransferer)(nil)

// ReadOnlyTransferer gets and posts manifest to tracker, and transfers blobs as torrent.
type ReadOnlyTransferer struct {
	stats tally.Scope
	cads  *store.CADownloadStore
	tags  tagclient.Client
	sched scheduler.Scheduler
}

// NewReadOnlyTransferer creates a new ReadOnlyTransferer.
func NewReadOnlyTransferer(
	stats tally.Scope,
	cads *store.CADownloadStore,
	tags tagclient.Client,
	sched scheduler.Scheduler,
) *ReadOnlyTransferer {
	stats = stats.Tagged(map[string]string{
		"module": "rotransferer",
	})

	return &ReadOnlyTransferer{stats, cads, tags, sched}
}

// mapSchedulerError converts scheduler errors to appropriate transferer errors.
func mapSchedulerError(err error, d core.Digest) error {
	// torrent not found → 404
	if err == scheduler.ErrTorrentNotFound {
		return ErrBlobNotFound{
			Digest: d.Hex(),
			Reason: "torrent not found in tracker",
		}
	}

	// All other scheduler errors → 500 with context
	return fmt.Errorf("download blob %s: %w", d.Hex(), err)
}

// Stat returns blob info from local cache, and triggers download if the blob is
// not available locally.
func (t *ReadOnlyTransferer) Stat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	fi, err := t.cads.Cache().GetFileStat(d.Hex())

	if err == nil {
		return core.NewBlobInfo(fi.Size()), nil
	}

	if !os.IsNotExist(err) && !t.cads.InDownloadError(err) {
		return nil, fmt.Errorf("stat cache: %w", err)
	}

	if err := t.sched.Download(namespace, d); err != nil {
		return nil, mapSchedulerError(err, d)
	}

	// Stat file after download completes
	// Use Any() to check both download and cache directories, as the file
	// might still be in the process of being moved from download to cache.
	fi, err = t.cads.Any().GetFileStat(d.Hex())
	if err == nil {
		return core.NewBlobInfo(fi.Size()), nil
	}
	if os.IsNotExist(err) {
		return nil, ErrBlobNotFound{
			Digest: d.Hex(),
			Reason: "file not found after download",
		}
	}
	return nil, fmt.Errorf("stat cache after download: %w", err)
}

// Download downloads blobs as torrent.
func (t *ReadOnlyTransferer) Download(namespace string, d core.Digest) (store.FileReader, error) {
	f, err := t.cads.Cache().GetFileReader(d.Hex())

	if err == nil {
		return f, nil
	}

	if !os.IsNotExist(err) && !t.cads.InDownloadError(err) {
		return nil, fmt.Errorf("get cache file: %w", err)
	}

	if err := t.sched.Download(namespace, d); err != nil {
		return nil, mapSchedulerError(err, d)
	}

	// Get file reader after download completes
	// Use Any() to check both download and cache directories, as the file
	// might still be in the process of being moved from download to cache.
	f, err = t.cads.Any().GetFileReader(d.Hex())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrBlobNotFound{
				Digest: d.Hex(),
				Reason: "file not found on disk after download",
			}
		}
		return nil, fmt.Errorf("get file reader after download: %w", err)
	}

	return f, nil
}

// Upload uploads blobs to a torrent network.
func (t *ReadOnlyTransferer) Upload(namespace string, d core.Digest, blob store.FileReader) error {
	return errors.New("unsupported operation")
}

// GetTag gets manifest digest for tag.
func (t *ReadOnlyTransferer) GetTag(tag string) (core.Digest, error) {
	d, err := t.tags.Get(tag)
	if err == nil {
		return d, nil
	}
	if err == tagclient.ErrTagNotFound {
		t.stats.Counter("tag_not_found").Inc(1)
		return core.Digest{}, ErrTagNotFound{
			Tag:    tag,
			Reason: "not found in build-index",
		}
	}
	t.stats.Counter("get_tag_error").Inc(1)
	return core.Digest{}, fmt.Errorf("client get tag: %w", err)
}

// PutTag is not supported.
func (t *ReadOnlyTransferer) PutTag(tag string, d core.Digest) error {
	return errors.New("not supported")
}

// ListTags is not supported.
func (t *ReadOnlyTransferer) ListTags(prefix string) ([]string, error) {
	return nil, errors.New("not supported")
}
