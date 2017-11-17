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
	miRaw, err := a.store.States().Download().Cache().GetMetadata(name, store.NewTorrentMeta())
	if os.IsNotExist(err) {
		mi, err := a.metaInfoClient.Download(name)
		if err != nil {
			if err == metainfoclient.ErrNotFound {
				return nil, os.ErrNotExist
			}
			return nil, fmt.Errorf("download metainfo: %s", err)
		}
		// There's a race condition here, but it's "okay"... Basically, we could
		// initialize a download file with metainfo that is rejected by file store,
		// because someone else beats us to it. However, we catch a lucky break
		// because the only piece of metainfo we use is file length -- which digest
		// (i.e. name) is derived from, so it's "okay".
		if err := a.store.EnsureDownloadOrCacheFilePresent(mi.Name(), mi.Info.Length); err != nil {
			return nil, fmt.Errorf("ensure download/cache file present: %s", err)
		}
		miRaw, err = mi.Serialize()
		if err != nil {
			return nil, fmt.Errorf("serialize downloaded metainfo: %s", err)
		}
		miRaw, err = a.store.States().Cache().Download().GetOrSetMetadata(
			name, store.NewTorrentMeta(), miRaw)
		if err != nil {
			return nil, fmt.Errorf("get or set metainfo metadata: %s", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("get download metadata: %s", err)
	}
	mi, err := torlib.DeserializeMetaInfo(miRaw)
	if err != nil {
		return nil, fmt.Errorf("parse metainfo: %s", err)
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
