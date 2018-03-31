package storage

import (
	"errors"
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/willf/bitset"
)

// OriginTorrentArchive is a TorrentArchive for origin peers. It assumes that
// all files (including metainfo) are already downloaded and in the cache directory.
type OriginTorrentArchive struct {
	fs            store.OriginFileStore
	blobRefresher *blobrefresh.Refresher
}

// NewOriginTorrentArchive creates a new OriginTorrentArchive.
func NewOriginTorrentArchive(
	fs store.OriginFileStore, blobRefresher *blobrefresh.Refresher) *OriginTorrentArchive {

	return &OriginTorrentArchive{fs, blobRefresher}
}

// Stat returns TorrentInfo for given file name. If the file does not exist,
// attempts to re-fetch the file from the storae backend configured for namespace
// in a background goroutine.
func (a *OriginTorrentArchive) Stat(namespace, name string) (*TorrentInfo, error) {
	raw, err := a.fs.GetCacheFileMetadata(name, store.NewTorrentMeta())
	if err != nil {
		if os.IsNotExist(err) {
			refreshErr := a.blobRefresher.Refresh(namespace, core.NewSHA256DigestFromHex(name))
			if refreshErr != nil {
				return nil, fmt.Errorf("blob refresh: %s", refreshErr)
			}
			log.With("name", name).Infof("Missing torrent triggered remote blob refresh")
			return nil, errors.New("refreshing blob")
		}
		return nil, err
	}
	mi, err := core.DeserializeMetaInfo(raw)
	if err != nil {
		return nil, fmt.Errorf("deserialize metainfo: %s", err)
	}

	bitfield := bitset.New(uint(mi.Info.NumPieces())).Complement()

	return newTorrentInfo(mi, bitfield), nil
}

// CreateTorrent is not supported.
func (a *OriginTorrentArchive) CreateTorrent(namespace, name string) (Torrent, error) {
	return nil, errors.New("not supported for origin")
}

// GetTorrent returns a Torrent for an existing file on disk. If the file does
// not exist, attempts to re-fetch the file from the storae backend configured
// for namespace in a background goroutine, and returns os.ErrNotExist.
func (a *OriginTorrentArchive) GetTorrent(namespace, name string) (Torrent, error) {
	info, err := a.Stat(namespace, name)
	if err != nil {
		return nil, fmt.Errorf("torrent stat: %s", err)
	}
	t, err := NewOriginTorrent(a.fs, info.metainfo)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// DeleteTorrent moves a torrent to the trash.
func (a *OriginTorrentArchive) DeleteTorrent(name string) error {
	if err := a.fs.DeleteCacheFile(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
