package storage

import (
	"errors"
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
)

// OriginTorrentArchive is a TorrentArchive for origin peers. It assumes that
// all files are already downloaded and in the cache directory.
type OriginTorrentArchive struct {
	fs             store.FileStore
	metaInfoClient metainfoclient.Client
}

// NewOriginTorrentArchive creates a new OriginTorrentArchive.
func NewOriginTorrentArchive(
	fs store.FileStore, metaInfoClient metainfoclient.Client) *OriginTorrentArchive {

	return &OriginTorrentArchive{fs, metaInfoClient}
}

// Stat returns TorrentInfo for given file name. Returns error if the file does
// not exist.
func (a *OriginTorrentArchive) Stat(name string) (*TorrentInfo, error) {
	cache := a.fs.States().Cache()

	// Ensure cache file is present.
	_, err := a.fs.GetCacheFileStat(name)
	if err != nil {
		return nil, fmt.Errorf("get cache file stat: %s", err)
	}

	// Get metainfo from disk, or download if not present. Metainfo will not be
	// present when the file is first pushed to the origin, and so we pull it
	// lazily.
	raw, err := cache.GetMetadata(name, store.NewTorrentMeta())
	if os.IsNotExist(err) {
		mi, err := a.metaInfoClient.Download(name)
		if err != nil {
			return nil, fmt.Errorf("download metainfo: %s", err)
		}
		raw, err = mi.Serialize()
		if err != nil {
			return nil, fmt.Errorf("serialize downloaded metainfo: %s", err)
		}
		if _, err := cache.GetOrSetMetadata(name, store.NewTorrentMeta(), raw); err != nil {
			return nil, fmt.Errorf("get or set metainfo metadata: %s", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("get metainfo metadata: %s", err)
	}
	mi, err := torlib.DeserializeMetaInfo(raw)
	if err != nil {
		return nil, fmt.Errorf("deserialize metainfo: %s", err)
	}

	bitfield := make([]bool, mi.Info.NumPieces())
	for i := range bitfield {
		bitfield[i] = true
	}

	return newTorrentInfo(mi, bitfield), nil
}

// GetTorrent returns a Torrent for an existing file on disk. Returns error if
// the file does not exist.
func (a *OriginTorrentArchive) GetTorrent(name string) (Torrent, error) {
	info, err := a.Stat(name)
	if err != nil {
		return nil, fmt.Errorf("torrent stat: %s", err)
	}

	// TODO(codyg): Return a read-only torrent implementation.
	t, err := NewLocalTorrent(a.fs, info.metainfo)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// DeleteTorrent moves a torrent to the trash.
func (a *OriginTorrentArchive) DeleteTorrent(name string) error {
	return errors.New("not supported for origin")
}
