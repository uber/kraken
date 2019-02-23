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
package blobrefresh

import (
	"errors"
	"fmt"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/metainfogen"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/utils/dedup"
	"github.com/uber/kraken/utils/log"

	"github.com/andres-erbsen/clock"
	"github.com/c2h5oh/datasize"
	"github.com/uber-go/tally"
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
	cas               *store.CAStore
	backends          *backend.Manager
	metaInfoGenerator *metainfogen.Generator
}

// New creates a new Refresher.
func New(
	config Config,
	stats tally.Scope,
	cas *store.CAStore,
	backends *backend.Manager,
	metaInfoGenerator *metainfogen.Generator) *Refresher {

	stats = stats.Tagged(map[string]string{
		"module": "blobrefresh",
	})

	requests := dedup.NewRequestCache(dedup.RequestCacheConfig{}, clock.New())
	requests.SetNotFound(func(err error) bool { return err == backenderrors.ErrBlobNotFound })

	return &Refresher{config, stats, requests, cas, backends, metaInfoGenerator}
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

	id := namespace + ":" + d.Hex()
	err = r.requests.Start(id, func() error {
		start := time.Now()
		if err := r.download(client, namespace, d); err != nil {
			return err
		}
		t := time.Since(start)
		r.stats.Timer("download_remote_blob").Record(t)
		log.With(
			"namespace", namespace,
			"name", d.Hex(),
			"download_time", t).Info("Downloaded remote blob")

		if err := r.metaInfoGenerator.Generate(d); err != nil {
			return fmt.Errorf("generate metainfo: %s", err)
		}
		r.stats.Counter("downloads").Inc(1)
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

func (r *Refresher) download(client backend.Client, namespace string, d core.Digest) error {
	name := d.Hex()
	return r.cas.WriteCacheFile(name, func(w store.FileReadWriter) error {
		return client.Download(namespace, name, w)
	})
}
