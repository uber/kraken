package agentstorage

import (
	"fmt"
	"os"

	"github.com/uber-go/tally"
	"github.com/willf/bitset"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/store/metadata"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
)

// TorrentArchive is capable of initializing torrents in the download directory
// and serving torrents from either the download or cache directory.
type TorrentArchive struct {
	stats          tally.Scope
	fs             store.FileStore
	metaInfoClient metainfoclient.Client
}

// NewTorrentArchive creates a new TorrentArchive.
func NewTorrentArchive(
	stats tally.Scope,
	fs store.FileStore,
	mic metainfoclient.Client) *TorrentArchive {

	stats = stats.Tagged(map[string]string{
		"module": "agenttorrentarchive",
	})

	return &TorrentArchive{stats, fs, mic}
}

// Stat returns TorrentInfo for given file name. Returns os.ErrNotExist if the
// file does not exist. Ignores namespace.
func (a *TorrentArchive) Stat(namespace, name string) (*storage.TorrentInfo, error) {
	downloadOrCache := a.fs.States().Download().Cache()
	var tm metadata.TorrentMeta
	if err := downloadOrCache.GetMetadata(name, &tm); err != nil {
		return nil, err
	}
	var psm pieceStatusMetadata
	if err := downloadOrCache.GetMetadata(name, &psm); err != nil {
		return nil, err
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
func (a *TorrentArchive) CreateTorrent(namespace, name string) (storage.Torrent, error) {
	downloadOrCache := a.fs.States().Download().Cache()
	var tm metadata.TorrentMeta
	if err := downloadOrCache.GetMetadata(name, &tm); os.IsNotExist(err) {
		downloadTimer := a.stats.Timer("metainfo_download").Start()
		mi, err := a.metaInfoClient.Download(namespace, name)
		if err != nil {
			if err == metainfoclient.ErrNotFound {
				return nil, storage.ErrNotFound
			}
			return nil, fmt.Errorf("download metainfo: %s", err)
		}
		downloadTimer.Stop()

		// There's a race condition here, but it's "okay"... Basically, we could
		// initialize a download file with metainfo that is rejected by file store,
		// because someone else beats us to it. However, we catch a lucky break
		// because the only piece of metainfo we use is file length -- which digest
		// (i.e. name) is derived from, so it's "okay".
		if err := a.fs.EnsureDownloadOrCacheFilePresent(mi.Name(), mi.Info.Length); err != nil {
			return nil, fmt.Errorf("ensure download/cache file present: %s", err)
		}
		tm.MetaInfo = mi
		if err := downloadOrCache.GetOrSetMetadata(name, &tm); err != nil {
			return nil, fmt.Errorf("get or set metainfo: %s", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("get metainfo: %s", err)
	}
	t, err := NewTorrent(a.fs, tm.MetaInfo)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// GetTorrent returns a Torrent for an existing metainfo / file on disk. Ignores namespace.
func (a *TorrentArchive) GetTorrent(namespace, name string) (storage.Torrent, error) {
	downloadOrCache := a.fs.States().Download().Cache()
	var tm metadata.TorrentMeta
	if err := downloadOrCache.GetMetadata(name, &tm); err != nil {
		return nil, fmt.Errorf("get metainfo: %s", err)
	}
	t, err := NewTorrent(a.fs, tm.MetaInfo)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// DeleteTorrent deletes a torrent from disk.
func (a *TorrentArchive) DeleteTorrent(name string) error {
	if err := a.fs.DeleteDownloadOrCacheFile(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
