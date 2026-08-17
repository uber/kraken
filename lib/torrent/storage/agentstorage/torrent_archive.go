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
package agentstorage

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/uber-go/tally"
	"github.com/willf/bitset"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/observability"
	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/tracker/metainfoclient"
	"github.com/uber/kraken/utils/closers"
)

// TorrentArchive is capable of initializing torrents in the download directory
// and serving torrents from either the download or cache directory.
type TorrentArchive struct {
	stats          tally.Scope
	store          *disk.Store
	metaInfoClient metainfoclient.Client
}

// NewTorrentArchive creates a new TorrentArchive.
func NewTorrentArchive(
	stats tally.Scope,
	store *disk.Store,
	mic metainfoclient.Client) *TorrentArchive {

	stats = stats.Tagged(map[string]string{
		"module": "agenttorrentarchive",
	})

	return &TorrentArchive{stats, store, mic}
}

// Stat returns TorrentInfo for the given digest. Returns os.ErrNotExist if the
// file does not exist. Ignores namespace.
func (a *TorrentArchive) Stat(namespace string, d core.Digest) (*storage.TorrentInfo, error) {
	var tm metadata.TorrentMeta
	ok, err := a.store.GetMetadata(d.Hex(), &tm)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, os.ErrNotExist
	}
	var psm pieceStatusMetadata
	ok, err = a.store.GetMetadata(d.Hex(), &psm)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, os.ErrNotExist
	}
	b := bitset.New(uint(len(psm.pieces)))
	for i, p := range psm.pieces {
		if p.status == _complete {
			b.Set(uint(i))
		}
	}
	return storage.NewTorrentInfo(tm.MetaInfo, b), nil
}

// CreateTorrent returns a Torrent for either an existing metainfo / file on
// disk, or downloads metainfo and initializes the file. Returns ErrNotFound
// if no metainfo was found.
func (a *TorrentArchive) CreateTorrent(namespace string, d core.Digest) (storage.Torrent, error) {
	var tm metadata.TorrentMeta
	ok, err := a.store.GetMetadata(d.Hex(), &tm)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("get metainfo: %s", err)
	}
	if errors.Is(err, os.ErrNotExist) || !ok {
		startTime := time.Now()
		mi, err := a.metaInfoClient.Download(namespace, d)
		if err != nil {
			if err == metainfoclient.ErrNotFound {
				return nil, storage.ErrNotFound
			}
			return nil, fmt.Errorf("download metainfo: %s", err)
		}
		metainfoDownloadLatency := time.Since(startTime)
		observability.EmitDownloadPerformance(a.stats, observability.METAINFO_DOWNLOAD, mi.Length(), metainfoDownloadLatency)

		// There's a race condition here, but it's "okay"... Basically, we could
		// initialize a download file with metainfo that is rejected by the store,
		// because someone else beats us to it. However, we catch a lucky break
		// because the only piece of metainfo we use is file length -- which digest
		// is derived from, so it's "okay".
		f, createErr := a.store.Create(mi.Digest().Hex(), uint64(mi.Length()))
		if createErr != nil {
			if !errors.Is(createErr, os.ErrExist) {
				return nil, fmt.Errorf("create download file: %s", createErr)
			}
		} else {
			closers.Close(f)
		}
		tm.MetaInfo = mi
		ok, err := a.store.GetMetadata(d.Hex(), &tm)
		if err != nil {
			return nil, fmt.Errorf("get or set metainfo: %s", err)
		}
		if !ok {
			if err := a.store.SetMetadata(d.Hex(), &tm); err != nil {
				return nil, fmt.Errorf("get or set metainfo: %s", err)
			}
		}
	}
	t, err := NewTorrent(a.store, tm.MetaInfo)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// GetTorrent returns a Torrent for an existing metainfo / file on disk. Ignores namespace.
func (a *TorrentArchive) GetTorrent(namespace string, d core.Digest) (storage.Torrent, error) {
	var tm metadata.TorrentMeta
	ok, err := a.store.GetMetadata(d.Hex(), &tm)
	if err != nil {
		return nil, fmt.Errorf("get metainfo: %s", err)
	}
	if !ok {
		return nil, fmt.Errorf("get metainfo: %w", os.ErrNotExist)
	}
	t, err := NewTorrent(a.store, tm.MetaInfo)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// DeleteTorrent deletes a torrent from disk.
func (a *TorrentArchive) DeleteTorrent(d core.Digest) error {
	if err := a.store.Delete(d.Hex()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
