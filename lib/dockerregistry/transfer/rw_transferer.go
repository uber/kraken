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
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/observability"
	storelib "github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/memsize"

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
	store         *disk.Store
	tracer        trace.Tracer
}

// NewReadWriteTransferer creates a new ReadWriteTransferer.
func NewReadWriteTransferer(
	stats tally.Scope,
	tags tagclient.Client,
	originCluster blobclient.ClusterClient,
	store *disk.Store,
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
		store:         store,
		tracer:        otel.Tracer("kraken-registry-transfer"),
	}
}

// Stat returns blob info from origin cluster or local cache.
func (t *ReadWriteTransferer) Stat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	fi, err := t.store.ScopeComplete().Stat(d.Hex())
	if err == nil {
		return core.NewBlobInfo(fi.Size()), nil
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, storelib.ErrOutOfScope) {
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
func (t *ReadWriteTransferer) Download(namespace string, d core.Digest) (storelib.FileReader, error) {
	start := time.Now()
	t.stats.Counter("download_requests").Inc(1)
	ctx, span := t.tracer.Start(context.Background(), "registry.download_blob",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("component", "registry-transfer"),
			attribute.String("operation", "download_blob"),
			attribute.String("namespace", namespace),
			attribute.String("blob.digest", d.Hex()),
		),
	)
	defer span.End()

	var blob storelib.FileReader
	blob, err := t.store.ScopeComplete().Open(d.Hex())
	if err == nil {
		span.SetAttributes(attribute.String("cache.status", "hit"))
		span.SetStatus(codes.Ok, "cache hit")
		mbServed := int64(uint64(blob.Size()) / memsize.MB)
		t.stats.Counter("mb_served").Inc(mbServed)
		observability.EmitDownloadPerformance(t.stats, observability.PROXY_BLOB_DOWNLOAD, blob.Size(), time.Since(start))
		return blob, nil
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, storelib.ErrOutOfScope) {
		span.SetAttributes(attribute.String("cache.status", "miss"))
		blob, err = t.downloadFromOrigin(ctx, namespace, d)
		if err != nil {
			t.stats.Counter("download_failures").Inc(1)
		} else {
			mbServed := int64(uint64(blob.Size()) / memsize.MB)
			t.stats.Counter("mb_served").Inc(mbServed)
			observability.EmitDownloadPerformance(t.stats, observability.PROXY_BLOB_DOWNLOAD, blob.Size(), time.Since(start))
		}
		return blob, err
	}
	t.stats.Counter("download_failures").Inc(1)
	span.RecordError(err)
	span.SetStatus(codes.Error, "cache read error")
	return nil, fmt.Errorf("get cache file: %s", err)
}

func (t *ReadWriteTransferer) downloadFromOrigin(ctx context.Context, namespace string, d core.Digest) (storelib.FileReader, error) {
	ctx, span := t.tracer.Start(ctx, "registry.download_from_origin",
		trace.WithAttributes(
			attribute.String("namespace", namespace),
			attribute.String("blob.digest", d.Hex()),
		),
	)
	defer span.End()

	// We use a tmp key instead of the d.Hex() directly, as this func needs
	// to return a reader to the newly downloaded data and if 2 routines
	// race to download from origin, the loser doesn't know when it can
	// return the opened file (it would need to poll for the download's completeness).
	// Thus we use tmp files to ensure every routine has its own blob to return a reader to.
	//
	// TODO - consider deduplicating these multiple downloads.
	tmp := fmt.Sprintf("%s.%s", d.Hex(), uuid.Generate().String())
	w, err := t.store.Create(tmp, 1)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to create upload file")
		return nil, fmt.Errorf("create upload file: %s", err)
	}
	defer closers.Close(w)
	if err := t.originCluster.DownloadBlob(ctx, namespace, d, w); err != nil {
		if err == blobclient.ErrBlobNotFound {
			span.SetStatus(codes.Error, "blob not found")
			return nil, ErrBlobNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "origin download failed")
		return nil, fmt.Errorf("origin: %s", err)
	}
	err = t.store.RenameKey(tmp, d.Hex())
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed rename file after upload")
			return nil, fmt.Errorf("rename key: %s", err)
		}
		// Another downloader already cached this digest; discard our copy.
		if err := t.store.Delete(tmp); err != nil {
			log.With("digest", d).Errorf("Leaked upload file: %s", err)
		}
	} else {
		err := t.store.MarkComplete(d.Hex())
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to mark complete")
			return nil, fmt.Errorf("mark complete: %s", err)
		}
	}
	blob, err := t.store.ScopeComplete().Open(d.Hex())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to open cached blob")
		return nil, fmt.Errorf("open file: %s", err)
	}

	span.SetStatus(codes.Ok, "download completed")
	return blob, nil
}

// Upload uploads blob to the origin cluster.
func (t *ReadWriteTransferer) Upload(
	namespace string, d core.Digest, blob storelib.FileReader,
) error {
	t.stats.Counter("upload_requests").Inc(1)
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

	if err := t.originCluster.UploadBlob(ctx, namespace, d, blob, uint64(blob.Size())); err != nil {
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
