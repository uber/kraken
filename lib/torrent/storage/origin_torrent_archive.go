package storage

import (
	"errors"
	"fmt"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"
)

// OriginTorrentArchive is a TorrentArchive for origin peers. It assumes that
// all files (including metainfo) are already downloaded and in the cache directory.
type OriginTorrentArchive struct {
	fs store.FileStore
}

// NewOriginTorrentArchive creates a new OriginTorrentArchive.
func NewOriginTorrentArchive(fs store.FileStore) *OriginTorrentArchive {
	return &OriginTorrentArchive{fs}
}

// Stat returns TorrentInfo for given file name. Returns error if the file does
// not exist.
func (a *OriginTorrentArchive) Stat(name string) (*TorrentInfo, error) {
	cache := a.fs.States().Cache()

	raw, err := cache.GetMetadata(name, store.NewTorrentMeta())
	if err != nil {
		return nil, fmt.Errorf("get metainfo: %s", err)
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

// CreateTorrent is not supported.
func (a *OriginTorrentArchive) CreateTorrent(namespace, name string) (Torrent, error) {
	return nil, errors.New("not supported for origin")
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
