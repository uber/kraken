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
package blobrefresh

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/metainfogen"
	"github.com/uber/kraken/lib/observability"
	"github.com/uber/kraken/lib/store/tiered"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/dedup"
	"github.com/uber/kraken/utils/log"

	"github.com/andres-erbsen/clock"
	"github.com/c2h5oh/datasize"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// Refresher errors.
var (
	ErrPending     = errors.New("download is pending")
	ErrNotFound    = errors.New("blob not found")
	ErrWorkersBusy = errors.New("no workers available")
)

// PostHook runs after the blob has been downloaded within the context of the
// deduplicated request.
type PostHook interface {
	Run(d core.Digest)
}

// Refresher deduplicates blob downloads / metainfo generation. Refresher is not
// responsible for tracking whether blobs already exist on disk -- it only provides
// a method for downloading blobs in a deduplicated fashion.
type Refresher struct {
	config            Config
	stats             tally.Scope
	requests          *dedup.RequestCache
	store             *tiered.Store
	backends          *backend.Manager
	metaInfoGenerator *metainfogen.Generator
}

// New creates a new Refresher.
func New(
	config Config,
	stats tally.Scope,
	store *tiered.Store,
	backends *backend.Manager,
	metaInfoGenerator *metainfogen.Generator) *Refresher {

	stats = stats.Tagged(map[string]string{
		"module": "blobrefresh",
	})

	requestsStats := stats.Tagged(map[string]string{
		"request_type": "blobrefresh",
	})
	requests := dedup.NewRequestCache(dedup.RequestCacheConfig{}, clock.New(), requestsStats)
	requests.SetNotFound(func(err error) bool { return err == backenderrors.ErrBlobNotFound })

	return &Refresher{config, stats, requests, store, backends, metaInfoGenerator}
}

// Refresh kicks off a background goroutine to download the blob for d from the
// remote backend configured for namespace and generates metainfo for the blob.
// Returns ErrPending if an existing download for the blob is already running.
// Returns ErrNotFound if the blob is not found. Returns ErrWorkersBusy if no
// goroutines are available to run the download.
func (r *Refresher) Refresh(namespace string, d core.Digest, hooks ...PostHook) error {
	client, err := r.backends.GetClient(namespace)
	if err != nil {
		return fmt.Errorf("backend manager: %s", err)
	}

	// Always check whether the blob is actually available and valid before
	// returning a potential pending error. This ensures that the majority of
	// errors are propogated quickly and syncronously.
	info, err := client.Stat(namespace, d.Hex())
	if err != nil {
		if err == backenderrors.ErrBlobNotFound {
			return ErrNotFound
		}
		return fmt.Errorf("stat: %s", err)
	}
	size := datasize.ByteSize(info.Size)
	if r.config.SizeLimit > 0 && size > r.config.SizeLimit {
		return fmt.Errorf("%s blob exceeds size limit of %s", size, r.config.SizeLimit)
	}

	id := d.Hex()
	err = r.requests.Start(id, func() error {
		err := r.download(client, namespace, d, size.Bytes())
		if err != nil {
			return err
		}
		for _, h := range hooks {
			h.Run(d)
		}
		return nil
	})
	switch err {
	case dedup.ErrRequestPending:
		return ErrPending
	case backenderrors.ErrBlobNotFound:
		return ErrNotFound
	case dedup.ErrWorkersBusy:
		return ErrWorkersBusy
	default:
		return err
	}
}

// downloadPhases records how long each stage of a remote blob download took.
// The total latency alone cannot tell apart a slow backend transfer from a
// download that finished writing and then waited on the store, so each stage is
// timed separately and logged on both the success and the failure paths.
type downloadPhases struct {
	create         time.Duration
	clientDownload time.Duration
	markComplete   time.Duration
	generate       time.Duration
}

func (p downloadPhases) logger(
	namespace string,
	d core.Digest,
	size uint64) *zap.SugaredLogger {

	return log.With(
		"namespace", namespace,
		"name", d.Hex(),
		"blob_size", size,
		"create_time", p.create,
		"client_download_time", p.clientDownload,
		"mark_complete_time", p.markComplete,
		"generate_metainfo_time", p.generate)
}

func (r *Refresher) download(client backend.Client, namespace string, d core.Digest, size uint64) error {
	var phases downloadPhases

	start := time.Now()
	f, err := r.store.Create(d.Hex(), size)
	phases.create = time.Since(start)
	if errors.Is(err, os.ErrExist) {
		if _, complete := r.store.ScopeComplete().Has(d.Hex()); complete {
			// No-op - the blob is already downloaded by either a previous refresher request or origin blob replication.
			return nil
		}
		// The blob is being replicated from other origins. We need to wait.
		// If replication fails halfway-through, the disk.Store's leak collector will
		// remove it after a while, failing open. Until then, we will continue returning ErrPending.
		return ErrPending
	}
	if err != nil {
		return fmt.Errorf("store create: %w", err)
	}
	defer closers.Close(f)

	clientDownloadStart := time.Now()
	err = client.Download(namespace, d.Hex(), f)
	phases.clientDownload = time.Since(clientDownloadStart)
	if err != nil {
		phases.logger(namespace, d, size).With("error", err).
			Error("Remote blob download failed while transferring from the backend")
		tiered.Abort(r.store, d.Hex())
		return fmt.Errorf("client download: %w", err)
	}

	markCompleteStart := time.Now()
	err = r.store.MarkComplete(d.Hex())
	phases.markComplete = time.Since(markCompleteStart)
	if err != nil {
		phases.logger(namespace, d, size).With("error", err).
			Error("Remote blob download failed while marking the blob as complete")
		tiered.Abort(r.store, d.Hex())
		return fmt.Errorf("mark complete: %w", err)
	}

	generateStart := time.Now()
	err = r.metaInfoGenerator.Generate(d)
	phases.generate = time.Since(generateStart)
	if err != nil {
		phases.logger(namespace, d, size).With("error", err).
			Error("Remote blob download failed while generating metainfo")
		return fmt.Errorf("generate and store metainfo: %w", err)
	}

	downloadLatency := time.Since(start)
	observability.EmitDownloadPerformance(r.stats, observability.REMOTE_DOWNLOAD, int64(size), downloadLatency)
	phases.logger(namespace, d, size).
		With("download_time", downloadLatency).Info("Downloaded remote blob")
	return nil
}
