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
package originstorage

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/blobrefresh"
	"github.com/uber/kraken/lib/metainfosidecar"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/utils/log"

	"github.com/willf/bitset"
)

// TorrentArchive is a TorrentArchive for origin peers. Blobs already in the
// local cache are served warm. Blobs not in the cache are served cold: their
// metainfo is fetched from a backend sidecar and their pieces are lazily
// range-fetched from the backend on demand.
type TorrentArchive struct {
	cas              *store.CAStore
	backends         *backend.Manager
	blobRefresher    *blobrefresh.Refresher
	fetchConcurrency int

	mu       sync.Mutex
	partials map[core.Digest]*Torrent // In-flight cold torrents, shared across callers.
}

// NewTorrentArchive creates a new TorrentArchive. fetchConcurrency bounds how
// many pieces of a partial (cold) torrent are range-fetched from the backend
// at once; <=0 uses the default.
func NewTorrentArchive(
	cas *store.CAStore,
	backends *backend.Manager,
	blobRefresher *blobrefresh.Refresher,
	fetchConcurrency int) *TorrentArchive {

	return &TorrentArchive{
		cas:              cas,
		backends:         backends,
		blobRefresher:    blobRefresher,
		fetchConcurrency: fetchConcurrency,
		partials:         make(map[core.Digest]*Torrent),
	}
}

// loadMetaInfo returns the metainfo for d. When the blob is not in the local
// cache it attempts the cold path: fetch the metainfo sidecar from the backend
// and return a RangeDownloader (rd != nil) for lazy piece fetching. When
// neither cache nor sidecar is available it triggers a background blob refresh
// and returns an error, matching legacy behavior.
func (a *TorrentArchive) loadMetaInfo(
	namespace string, d core.Digest) (*core.MetaInfo, backend.RangeDownloader, error) {

	var tm metadata.TorrentMeta
	err := a.cas.GetCacheFileMetadata(d.Hex(), &tm)
	if err == nil {
		return tm.MetaInfo, nil, nil
	}
	if !os.IsNotExist(err) {
		return nil, nil, err
	}
	if mi, rd, ok := a.coldMetaInfo(namespace, d); ok {
		return mi, rd, nil
	}
	if refreshErr := a.blobRefresher.Refresh(namespace, d); refreshErr != nil {
		return nil, nil, fmt.Errorf("blob refresh: %s", refreshErr)
	}
	return nil, nil, errors.New("refreshing blob")
}

// coldMetaInfo fetches the metainfo sidecar for a cold blob from the backend.
// ok is false when the backend has no range support or no sidecar is present,
// in which case the caller falls back to a full blob refresh.
func (a *TorrentArchive) coldMetaInfo(
	namespace string, d core.Digest) (*core.MetaInfo, backend.RangeDownloader, bool) {

	client, err := a.backends.GetClient(namespace)
	if err != nil {
		return nil, nil, false
	}
	rd, ok := backend.AsRangeDownloader(client)
	if !ok {
		return nil, nil, false
	}
	mi, err := metainfosidecar.Fetch(client, namespace, d)
	if err != nil {
		log.With("namespace", namespace, "digest", d.Hex()).
			Debugf("Cold metainfo sidecar unavailable: %s", err)
		return nil, nil, false
	}
	return mi, rd, true
}

// CanStreamCold reports whether d can be served as a cold/partial torrent for
// namespace, i.e. the backend supports ranged downloads and a metainfo
// sidecar is present. Unlike Stat/GetTorrent on a miss, this never triggers a
// background whole-blob refresh -- callers use it as a pure capability check
// before deciding which download path to take.
func (a *TorrentArchive) CanStreamCold(namespace string, d core.Digest) bool {
	_, _, ok := a.coldMetaInfo(namespace, d)
	return ok
}

// Stat returns TorrentInfo for given digest. If the file is neither cached nor
// available as a cold sidecar, attempts to re-fetch the file from the storage
// backend configured for namespace in a background goroutine.
func (a *TorrentArchive) Stat(namespace string, d core.Digest) (*storage.TorrentInfo, error) {
	mi, _, err := a.loadMetaInfo(namespace, d)
	if err != nil {
		return nil, err
	}
	bitfield := bitset.New(uint(mi.NumPieces())).Complement()
	return storage.NewTorrentInfo(mi, bitfield), nil
}

// CreateTorrent is not supported.
func (a *TorrentArchive) CreateTorrent(namespace string, d core.Digest) (storage.Torrent, error) {
	return nil, errors.New("not supported for origin")
}

// GetTorrent returns a Torrent for an existing file on disk, or a partial
// Torrent backed by lazy backend range-fetches when the blob is cold. If the
// blob is neither cached nor available as a cold sidecar, attempts to re-fetch
// it in a background goroutine and returns an error.
//
// Cold/partial torrents are shared across concurrent callers of the same
// digest (background fetches, live P2P peer serving, range reads) so they
// coordinate piece state and cache promotion through one instance instead of
// racing independent ones -- see maybePromoteToCache/MoveDownloadFileToCache.
func (a *TorrentArchive) GetTorrent(namespace string, d core.Digest) (storage.Torrent, error) {
	mi, rd, err := a.loadMetaInfo(namespace, d)
	if err != nil {
		return nil, err
	}
	if rd == nil {
		// Warm: already fully cached. Stateless read, no coordination needed.
		t, err := NewTorrent(a.cas, mi)
		if err != nil {
			return nil, fmt.Errorf("initialize torrent: %s", err)
		}
		return t, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if t, ok := a.partials[d]; ok {
		if t.numComplete.Load() != int32(t.NumPieces()) {
			return t, nil
		}
		// Already fully fetched by a previous caller (and promoted to cache
		// via maybePromoteToCache) -- evict so this call falls through and
		// re-runs loadMetaInfo's cache-hit path instead of reading a stale
		// download file that's already been moved away.
		delete(a.partials, d)
		return a.getTorrentLocked(namespace, d)
	}
	return a.newPartialTorrentLocked(namespace, d, mi, rd)
}

// getTorrentLocked re-resolves d from scratch while a.mu is already held,
// following a stale-partial eviction in GetTorrent.
func (a *TorrentArchive) getTorrentLocked(namespace string, d core.Digest) (storage.Torrent, error) {
	mi, rd, err := a.loadMetaInfo(namespace, d)
	if err != nil {
		return nil, err
	}
	if rd == nil {
		t, err := NewTorrent(a.cas, mi)
		if err != nil {
			return nil, fmt.Errorf("initialize torrent: %s", err)
		}
		return t, nil
	}
	return a.newPartialTorrentLocked(namespace, d, mi, rd)
}

// newPartialTorrentLocked constructs and registers a new partial torrent for
// d. a.mu must be held, and a.partials[d] must not already exist.
func (a *TorrentArchive) newPartialTorrentLocked(
	namespace string, d core.Digest, mi *core.MetaInfo, rd backend.RangeDownloader) (storage.Torrent, error) {

	t, err := NewPartialTorrent(a.cas, rd, namespace, mi, a.fetchConcurrency)
	if err != nil {
		return nil, fmt.Errorf("initialize partial torrent: %s", err)
	}
	a.partials[d] = t
	return t, nil
}

// DeleteTorrent moves a torrent to the trash. Also cleans up any lingering
// cold/partial download-state file, since a cold torrent never lands in cas
// until it's fully promoted.
func (a *TorrentArchive) DeleteTorrent(d core.Digest) error {
	if err := a.cas.DeleteCacheFile(d.Hex()); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := a.cas.DeleteDownloadFile(d.Hex()); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
