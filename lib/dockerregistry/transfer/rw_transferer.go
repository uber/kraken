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
	"context"
	"fmt"
	"os"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"

	"github.com/docker/distribution/uuid"
	"github.com/uber-go/tally"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ReadWriteTransferer is a Transferer for proxy. Uploads/downloads blobs via the
// local origin cluster, and posts/gets tags via the local build-index.
type ReadWriteTransferer struct {
	stats         tally.Scope
	successStats  tally.Scope
	failureStats  tally.Scope
	tags          tagclient.Client
	originCluster blobclient.ClusterClient
	cas           *store.CAStore
	tracer        trace.Tracer
}

// NewReadWriteTransferer creates a new ReadWriteTransferer.
func NewReadWriteTransferer(
	stats tally.Scope,
	tags tagclient.Client,
	originCluster blobclient.ClusterClient,
	cas *store.CAStore,
) *ReadWriteTransferer {
	stats = stats.Tagged(map[string]string{
		"module": "rwtransferer",
	})

	return &ReadWriteTransferer{
		stats:         stats,
		successStats:  stats.Tagged(map[string]string{"result": "success"}),
		failureStats:  stats.Tagged(map[string]string{"result": "failure"}),
		tags:          tags,
		originCluster: originCluster,
		cas:           cas,
		tracer:        otel.Tracer("kraken-registry-transfer"),
	}
}

// Stat returns blob info from origin cluster or local cache.
func (t *ReadWriteTransferer) Stat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	fi, err := t.cas.GetCacheFileStat(d.Hex())
	if err == nil {
		return core.NewBlobInfo(fi.Size()), nil
	}
	if os.IsNotExist(err) {
		return t.originStat(namespace, d)
	}
	return nil, fmt.Errorf("stat cache file: %s", err)
}

func (t *ReadWriteTransferer) originStat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	bi, err := t.originCluster.Stat(namespace, d)
	if err == nil {
		return bi, nil
	}
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

// Download downloads the blob of name into the file store and returns a reader
// to the newly downloaded file.
func (t *ReadWriteTransferer) Download(namespace string, d core.Digest) (store.FileReader, error) {
	blob, err := t.cas.GetCacheFileReader(d.Hex())
	if err == nil {
		return blob, nil
	}
	if os.IsNotExist(err) {
		return t.downloadFromOrigin(namespace, d)
	}
	return nil, fmt.Errorf("get cache file: %s", err)
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
	defer closers.Close(w)
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
	namespace string, d core.Digest, blob store.FileReader,
) error {
	ctx, span := t.tracer.Start(context.Background(), "registry.upload_blob",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("component", "registry-transfer"),
			attribute.String("operation", "upload_blob"),
			attribute.String("namespace", namespace),
			attribute.String("blob.digest", d.Hex()),
		),
	)
	defer span.End()

	if err := t.originCluster.UploadBlob(ctx, namespace, d, blob); err != nil {
		t.failureStats.Counter("upload_blob").Inc(1)
		span.RecordError(err)
		span.SetStatus(codes.Error, "upload failed")
		return err
	}

	t.successStats.Counter("upload_blob").Inc(1)
	span.SetStatus(codes.Ok, "upload completed")
	return nil
}

// GetTag returns the manifest digest for tag.
func (t *ReadWriteTransferer) GetTag(tag string) (core.Digest, error) {
	d, err := t.tags.Get(tag)
	if err == nil {
		return d, nil
	}

	if err == tagclient.ErrTagNotFound {
		return core.Digest{}, ErrTagNotFound
	}
	return core.Digest{}, fmt.Errorf("client get tag: %s", err)
}

// PutTag uploads d as the manifest digest for tag.
func (t *ReadWriteTransferer) PutTag(tag string, d core.Digest) error {
	if err := t.tags.PutAndReplicate(tag, d); err != nil {
		t.failureStats.Counter("put_tag").Inc(1)
		return fmt.Errorf("put and replicate tag: %s", err)
	}
	t.successStats.Counter("put_tag").Inc(1)
	return nil
}

// ListTags lists all tags with prefix.
func (t *ReadWriteTransferer) ListTags(prefix string) ([]string, error) {
	return t.tags.List(prefix)
}
