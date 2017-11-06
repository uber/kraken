package storage

import (
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
)

var _ TorrentArchive = (*LocalTorrentArchive)(nil)

// LocalTorrentArchive implements TorrentArchive
type LocalTorrentArchive struct {
	store          store.FileStore
	metaInfoClient metainfoclient.Client
}

// NewLocalTorrentArchive creates a new LocalTorrentArchive
func NewLocalTorrentArchive(store store.FileStore, metaInfoClient metainfoclient.Client) *LocalTorrentArchive {
	return &LocalTorrentArchive{store, metaInfoClient}
}

// GetTorrent implements TorrentArchive.GetTorrent
// All torrents are content addressable by name, so both name and ih identify a unique torrent
// Our storage supports search by content addressable file name, so torrent name is required
// Returns os.ErrNotExist if the torrent does not exist.
func (a *LocalTorrentArchive) GetTorrent(name string) (Torrent, error) {
	var mi *torlib.MetaInfo

	miRaw, err := a.store.GetDownloadOrCacheFileMeta(name)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("get metainfo from disk: %s", err)
		}
		mi, err = a.metaInfoClient.Download(name)
		if err != nil {
			if err == metainfoclient.ErrNotFound {
				return nil, os.ErrNotExist
			}
			return nil, fmt.Errorf("download metainfo: %s", err)
		}
	} else {
		mi, err = torlib.DeserializeMetaInfo(miRaw)
		if err != nil {
			return nil, fmt.Errorf("parse metainfo: %s", err)
		}
	}
	t, err := NewLocalTorrent(a.store, mi)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// DeleteTorrent implements TorrentArchive.DeleteTorrent
func (a *LocalTorrentArchive) DeleteTorrent(name string) error {
	return a.store.MoveDownloadOrCacheFileToTrash(name)
}

// Close implements TorrentArchive.Close
func (a *LocalTorrentArchive) Close() error {
	return nil
}
