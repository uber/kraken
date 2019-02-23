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
package originstorage

import (
	"errors"
	"fmt"
	"os"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/blobrefresh"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/lib/torrent/storage"

	"github.com/willf/bitset"
)

// TorrentArchive is a TorrentArchive for origin peers. It assumes that
// all files (including metainfo) are already downloaded and in the cache directory.
type TorrentArchive struct {
	cas           *store.CAStore
	blobRefresher *blobrefresh.Refresher
}

// NewTorrentArchive creates a new TorrentArchive.
func NewTorrentArchive(
	cas *store.CAStore, blobRefresher *blobrefresh.Refresher) *TorrentArchive {

	return &TorrentArchive{cas, blobRefresher}
}

func (a *TorrentArchive) getMetaInfo(namespace string, d core.Digest) (*core.MetaInfo, error) {
	var tm metadata.TorrentMeta
	if err := a.cas.GetCacheFileMetadata(d.Hex(), &tm); err != nil {
		if os.IsNotExist(err) {
			refreshErr := a.blobRefresher.Refresh(namespace, d)
			if refreshErr != nil {
				return nil, fmt.Errorf("blob refresh: %s", refreshErr)
			}
			return nil, errors.New("refreshing blob")
		}
		return nil, err
	}
	return tm.MetaInfo, nil
}

// Stat returns TorrentInfo for given digest. If the file does not exist,
// attempts to re-fetch the file from the storae backend configured for namespace
// in a background goroutine.
func (a *TorrentArchive) Stat(namespace string, d core.Digest) (*storage.TorrentInfo, error) {
	mi, err := a.getMetaInfo(namespace, d)
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

// GetTorrent returns a Torrent for an existing file on disk. If the file does
// not exist, attempts to re-fetch the file from the storae backend configured
// for namespace in a background goroutine, and returns os.ErrNotExist.
func (a *TorrentArchive) GetTorrent(namespace string, d core.Digest) (storage.Torrent, error) {
	mi, err := a.getMetaInfo(namespace, d)
	if err != nil {
		return nil, err
	}
	t, err := NewTorrent(a.cas, mi)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// DeleteTorrent moves a torrent to the trash.
func (a *TorrentArchive) DeleteTorrent(d core.Digest) error {
	if err := a.cas.DeleteCacheFile(d.Hex()); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
