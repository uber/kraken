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
package blobclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/utils/errutil"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Locations queries cluster for the locations of d.
func Locations(p Provider, cluster hostlist.List, d core.Digest) (locs []string, err error) {
	addrs := cluster.Resolve().Sample(3)
	if len(addrs) == 0 {
		return nil, errors.New("cluster is empty")
	}
	for addr := range addrs {
		locs, err = p.Provide(addr).Locations(d)
		if err != nil {
			continue
		}
		break
	}
	return locs, err
}

// ClientResolver resolves digests into Clients of origins.
type ClientResolver interface {
	// Resolve must return an ordered, stable, non-empty list of Clients for origins owning d.
	Resolve(d core.Digest) ([]Client, error)
}

type clientResolver struct {
	provider Provider
	cluster  hostlist.List
}

// NewClientResolver returns a new client resolver.
func NewClientResolver(p Provider, cluster hostlist.List) ClientResolver {
	return &clientResolver{p, cluster}
}

func (r *clientResolver) Resolve(d core.Digest) ([]Client, error) {
	locs, err := Locations(r.provider, r.cluster, d)
	if err != nil {
		return nil, err
	}
	var clients []Client
	for _, loc := range locs {
		clients = append(clients, r.provider.Provide(loc))
	}
	return clients, nil
}

var _ ClusterClient = &clusterClient{}

// ClusterClient defines a top-level origin cluster client which handles blob
// location resolution and retries.
type ClusterClient interface {
	CheckReadiness() error
	UploadBlob(ctx context.Context, namespace string, d core.Digest, blob io.ReadSeeker) error
	DownloadBlob(ctx context.Context, namespace string, d core.Digest, dst io.Writer) error
	PrefetchBlob(namespace string, d core.Digest) error
	GetMetaInfo(namespace string, d core.Digest) (*core.MetaInfo, error)
	Stat(namespace string, d core.Digest) (*core.BlobInfo, error)
	OverwriteMetaInfo(d core.Digest, pieceLength int64) error
	Owners(d core.Digest) ([]core.PeerContext, error)
	ReplicateToRemote(namespace string, d core.Digest, remoteDNS string) error
}

type clusterClient struct {
	resolver ClientResolver
}

// NewClusterClient returns a new ClusterClient.
func NewClusterClient(r ClientResolver) ClusterClient {
	return &clusterClient{r}
}

// defaultPollBackOff returns the default backoff used on Poll operations.
func (c *clusterClient) defaultPollBackOff() backoff.BackOff {
	return &backoff.ExponentialBackOff{
		InitialInterval:     time.Second,
		RandomizationFactor: 0.05,
		Multiplier:          1.3,
		MaxInterval:         5 * time.Second,
		MaxElapsedTime:      15 * time.Minute,
		Clock:               backoff.SystemClock,
	}
}

func (c *clusterClient) CheckReadiness() error {
	clients, err := c.resolver.Resolve(backend.ReadinessCheckDigest)
	if err != nil {
		return fmt.Errorf("resolve clients: %s", err)
	}
	randIdx := rand.Intn(len(clients))
	return clients[randIdx].CheckReadiness()
}

// UploadBlob uploads blob to origin cluster. See Client.UploadBlob for more details.
func (c *clusterClient) UploadBlob(ctx context.Context, namespace string, d core.Digest, blob io.ReadSeeker) (err error) {
	ctx, span := otel.Tracer("kraken-origin-cluster").Start(ctx, "cluster.upload_blob",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("component", "origin-cluster-client"),
			attribute.String("operation", "upload_blob"),
			attribute.String("namespace", namespace),
			attribute.String("blob.digest", d.Hex()),
		),
	)
	defer span.End()

	clients, err := c.resolver.Resolve(d)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to resolve clients")
		return fmt.Errorf("resolve clients: %s", err)
	}

	span.SetAttributes(attribute.Int("cluster.origin_count", len(clients)))
	log.WithTraceContext(ctx).With("namespace", namespace, "digest", d.Hex()).Debug("Starting blob upload to origin cluster")

	// We prefer the origin with highest hashing score so the first origin will handle
	// replication to origins with lower score. This is because we want to reduce upload
	// conflicts between local replicas.
	for i, client := range clients {
		originAddr := client.Addr()
		span.SetAttributes(attribute.Int("cluster.attempt", i))

		log.WithTraceContext(ctx).With("namespace", namespace, "digest", d.Hex(), "origin", originAddr, "attempt", i).Debug("Attempting blob upload to origin")
		err = client.UploadBlob(ctx, namespace, d, blob)
		if err == nil {
			log.WithTraceContext(ctx).With("namespace", namespace, "digest", d.Hex(), "origin", originAddr).Debug("Blob upload succeeded")
			span.SetAttributes(attribute.String("cluster.successful_origin", originAddr))
			span.SetStatus(codes.Ok, "upload succeeded")
			return nil
		}
		log.WithTraceContext(ctx).With("namespace", namespace, "digest", d.Hex(), "origin", originAddr, "error", err).Error("Blob upload failed")

		// Non-retryable error - don't try other origins
		if !httputil.IsNetworkError(err) && !httputil.IsRetryable(err) {
			span.RecordError(err)
			span.SetStatus(codes.Error, "non-retryable error")
			return err
		}

		// Allow retry on another origin if the current upstream is temporarily
		// unavailable or under high load.
		log.WithTraceContext(ctx).With("namespace", namespace, "digest", d.Hex(), "origin", originAddr, "attempt", i).Debug("Rewinding blob reader for retry")
		if _, seekErr := blob.Seek(0, io.SeekStart); seekErr != nil {
			log.WithTraceContext(ctx).With("namespace", namespace, "digest", d.Hex(), "error", seekErr).Error("Failed to rewind blob reader for retry")
			span.RecordError(seekErr)
			span.SetStatus(codes.Error, "failed to rewind blob")
			return fmt.Errorf("rewind blob for retry after %d attempts: %w", i, seekErr)
		}
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, "all origins failed")
	return err
}

// GetMetaInfo returns the metainfo for d. Does not handle polling.
func (c *clusterClient) GetMetaInfo(namespace string, d core.Digest) (mi *core.MetaInfo, err error) {
	clients, err := c.resolver.Resolve(d)
	if err != nil {
		return nil, fmt.Errorf("resolve clients: %s", err)
	}
	for _, client := range clients {
		mi, err = client.GetMetaInfo(namespace, d)
		// Do not try the next replica on 202 errors.
		if err != nil && !httputil.IsAccepted(err) {
			continue
		}
		break
	}
	return mi, err
}

// Stat checks availability of a blob in the cluster.
func (c *clusterClient) Stat(namespace string, d core.Digest) (bi *core.BlobInfo, err error) {
	clients, err := c.resolver.Resolve(d)
	if err != nil {
		return nil, fmt.Errorf("resolve clients: %s", err)
	}

	shuffle(clients)
	for _, client := range clients {
		bi, err = client.Stat(namespace, d)
		if err != nil {
			continue
		}
		break
	}

	return bi, err
}

// OverwriteMetaInfo overwrites existing metainfo for d with new metainfo configured
// with pieceLength on every origin server. Returns error if any origin was unable
// to overwrite metainfo. Primarly intended for benchmarking purposes.
func (c *clusterClient) OverwriteMetaInfo(d core.Digest, pieceLength int64) error {
	clients, err := c.resolver.Resolve(d)
	if err != nil {
		return fmt.Errorf("resolve clients: %s", err)
	}
	var errs []error
	for _, client := range clients {
		if err := client.OverwriteMetaInfo(d, pieceLength); err != nil {
			errs = append(errs, fmt.Errorf("origin %s: %s", client.Addr(), err))
		}
	}
	return errutil.Join(errs)
}

// DownloadBlob pulls a blob from the origin cluster.
func (c *clusterClient) DownloadBlob(ctx context.Context, namespace string, d core.Digest, dst io.Writer) error {
	ctx, span := otel.Tracer("kraken-origin-cluster").Start(ctx, "cluster.download_blob",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("component", "origin-cluster-client"),
			attribute.String("operation", "download_blob"),
			attribute.String("namespace", namespace),
			attribute.String("blob.digest", d.Hex()),
		),
	)
	defer span.End()

	log.WithTraceContext(ctx).With("namespace", namespace, "digest", d.Hex()).Debug("Starting blob download from origin cluster")

	err := Poll(c.resolver, c.defaultPollBackOff(), d, func(client Client) error {
		return client.DownloadBlob(ctx, namespace, d, dst)
	})
	if httputil.IsNotFound(err) {
		span.SetStatus(codes.Error, "blob not found")
		err = ErrBlobNotFound
	} else if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "download failed")
		log.WithTraceContext(ctx).With("namespace", namespace, "digest", d.Hex(), "error", err).Error("Blob download failed")
	} else {
		span.SetStatus(codes.Ok, "download completed")
		log.WithTraceContext(ctx).With("namespace", namespace, "digest", d.Hex()).Debug("Blob download succeeded")
	}
	return err
}

// PrefetchBlob preheats a blob in the origin cluster for downloading.
// Check [Client].PrefetchBlob's comment for more info.
func (c *clusterClient) PrefetchBlob(namespace string, d core.Digest) error {
	clients, err := c.resolver.Resolve(d)
	if err != nil {
		return fmt.Errorf("resolve clients: %w", err)
	}

	var errs []error
	for _, client := range clients {
		err = client.PrefetchBlob(namespace, d)
		if err == nil {
			return nil
		}

		if httputil.IsNotFound(err) {
			// no need to iterate over other origins
			return ErrBlobNotFound
		}
		errs = append(errs, err)
	}

	return fmt.Errorf("all origins unavailable: %w", errors.Join(errs...))
}

// Owners returns the origin peers which own d.
func (c *clusterClient) Owners(d core.Digest) ([]core.PeerContext, error) {
	clients, err := c.resolver.Resolve(d)
	if err != nil {
		return nil, fmt.Errorf("resolve clients: %s", err)
	}

	var mu sync.Mutex
	var peers []core.PeerContext
	var errs []error

	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		go func(client Client) {
			defer wg.Done()
			pctx, err := client.GetPeerContext()
			mu.Lock()
			if err != nil {
				errs = append(errs, err)
			} else {
				peers = append(peers, pctx)
			}
			mu.Unlock()
		}(client)
	}
	wg.Wait()

	err = errutil.Join(errs)

	if len(peers) == 0 {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("no origin peers found")
	}

	if err != nil {
		log.With("blob", d.Hex()).Errorf("Error getting all origin peers: %s", err)
	}
	return peers, nil
}

// ReplicateToRemote replicates d to a remote origin cluster.
func (c *clusterClient) ReplicateToRemote(namespace string, d core.Digest, remoteDNS string) error {
	// Re-use download backoff since replicate may download blobs.
	return Poll(c.resolver, c.defaultPollBackOff(), d, func(client Client) error {
		return client.ReplicateToRemote(namespace, d, remoteDNS)
	})
}

func shuffle(cs []Client) {
	for i := range cs {
		j := rand.Intn(i + 1)
		cs[i], cs[j] = cs[j], cs[i]
	}
}

// Poll wraps requests for endpoints which require polling, due to a blob
// being asynchronously fetched from remote storage in the origin cluster.
func Poll(
	r ClientResolver, b backoff.BackOff, d core.Digest, makeRequest func(Client) error,
) error {
	// By looping over clients in order, we will always prefer the same origin
	// for making requests to loosely guarantee that only one origin needs to
	// fetch the file from remote backend.
	clients, err := r.Resolve(d)
	if err != nil {
		return fmt.Errorf("resolve clients: %s", err)
	}
	var errs []error
ORIGINS:
	for _, client := range clients {
		b.Reset()
	POLL:
		for {
			if err := makeRequest(client); err != nil {
				if serr, ok := err.(httputil.StatusError); ok {
					if serr.Status == http.StatusAccepted {
						d := b.NextBackOff()
						if d == backoff.Stop {
							break POLL // Backoff timed out.
						}
						time.Sleep(d)
						continue POLL
					}
					if serr.Status < 500 {
						return err
					}
				}
				errs = append(errs, fmt.Errorf("origin %s: %s", client.Addr(), err))
				continue ORIGINS
			}
			return nil // Success!
		}
		errs = append(errs,
			fmt.Errorf("origin %s: backoff timed out on 202 responses", client.Addr()))
	}
	return fmt.Errorf("all origins unavailable: %s", errutil.Join(errs))
}
