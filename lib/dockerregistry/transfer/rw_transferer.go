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
package transfer

import (
	"fmt"
	"os"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/log"

	"github.com/docker/distribution/uuid"
	"github.com/uber-go/tally"
)

// ReadWriteTransferer is a Transferer for proxy. Uploads/downloads blobs via the
// local origin cluster, and posts/gets tags via the local build-index.
type ReadWriteTransferer struct {
	stats         tally.Scope
	tags          tagclient.Client
	originCluster blobclient.ClusterClient
	cas           *store.CAStore
}

// NewReadWriteTransferer creates a new ReadWriteTransferer.
func NewReadWriteTransferer(
	stats tally.Scope,
	tags tagclient.Client,
	originCluster blobclient.ClusterClient,
	cas *store.CAStore) *ReadWriteTransferer {

	stats = stats.Tagged(map[string]string{
		"module": "rwtransferer",
	})

	return &ReadWriteTransferer{stats, tags, originCluster, cas}
}

// Stat returns blob info from origin cluster or local cache.
func (t *ReadWriteTransferer) Stat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	fi, err := t.cas.GetCacheFileStat(d.Hex())
	if err != nil {
		if os.IsNotExist(err) {
			return t.originStat(namespace, d)
		}
		return nil, fmt.Errorf("stat cache file: %s", err)
	}
	return core.NewBlobInfo(fi.Size()), nil
}

func (t *ReadWriteTransferer) originStat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	bi, err := t.originCluster.Stat(namespace, d)
	if err != nil {
		// `docker push` stats blobs before uploading them. If the blob is not
		// found, it will upload it. However if remote blob storage is unavailable,
		// this will be a 5XX error, and will short-circuit push. We must consider
		// this class of error to be a 404 to allow pushes to succeed while remote
		// storage is down (write-back will eventually persist the blobs).
		if err != blobclient.ErrBlobNotFound {
			log.With("digest", d).Info("Error stat-ing origin blob: %s", err)
		}
		return nil, ErrBlobNotFound
	}
	return bi, nil
}

// Download downloads the blob of name into the file store and returns a reader
// to the newly downloaded file.
func (t *ReadWriteTransferer) Download(namespace string, d core.Digest) (store.FileReader, error) {
	blob, err := t.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		if os.IsNotExist(err) {
			return t.downloadFromOrigin(namespace, d)
		}
		return nil, fmt.Errorf("get cache file: %s", err)
	}
	return blob, nil
}

func (t *ReadWriteTransferer) downloadFromOrigin(namespace string, d core.Digest) (store.FileReader, error) {
	tmp := fmt.Sprintf("%s.%s", d.Hex(), uuid.Generate().String())
	if err := t.cas.CreateUploadFile(tmp, 0); err != nil {
		return nil, fmt.Errorf("create upload file: %s", err)
	}
	w, err := t.cas.GetUploadFileReadWriter(tmp)
	if err != nil {
		return nil, fmt.Errorf("get upload writer: %s", err)
	}
	defer w.Close()
	if err := t.originCluster.DownloadBlob(namespace, d, w); err != nil {
		if err == blobclient.ErrBlobNotFound {
			return nil, ErrBlobNotFound
		}
		return nil, fmt.Errorf("origin: %s", err)
	}
	if err := t.cas.MoveUploadFileToCache(tmp, d.Hex()); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("move upload file to cache: %s", err)
	}
	blob, err := t.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		return nil, fmt.Errorf("get cache file: %s", err)
	}
	return blob, nil
}

// Upload uploads blob to the origin cluster.
func (t *ReadWriteTransferer) Upload(
	namespace string, d core.Digest, blob store.FileReader) error {

	return t.originCluster.UploadBlob(namespace, d, blob)
}

// GetTag returns the manifest digest for tag.
func (t *ReadWriteTransferer) GetTag(tag string) (core.Digest, error) {
	d, err := t.tags.Get(tag)
	if err != nil {
		if err == tagclient.ErrTagNotFound {
			return core.Digest{}, ErrTagNotFound
		}
		return core.Digest{}, fmt.Errorf("client get tag: %s", err)
	}
	return d, nil
}

// PutTag uploads d as the manifest digest for tag.
func (t *ReadWriteTransferer) PutTag(tag string, d core.Digest) error {
	if err := t.tags.PutAndReplicate(tag, d); err != nil {
		t.stats.Counter("put_tag_error").Inc(1)
		return fmt.Errorf("put and replicate tag: %s", err)
	}
	return nil
}

// ListTags lists all tags with prefix.
func (t *ReadWriteTransferer) ListTags(prefix string) ([]string, error) {
	return t.tags.List(prefix)
}
