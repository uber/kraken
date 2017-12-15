package storage

import (
	"fmt"
	"os"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
)

// AgentTorrentArchive is a TorrentArchive for agent peers. It is capable
// of initializing torrents in the download directory and serving torrents
// from either the download or cache directory.
type AgentTorrentArchive struct {
	fs             store.FileStore
	metaInfoClient metainfoclient.Client
}

// NewAgentTorrentArchive creates a new AgentTorrentArchive
func NewAgentTorrentArchive(
	fs store.FileStore, metaInfoClient metainfoclient.Client) *AgentTorrentArchive {

	return &AgentTorrentArchive{fs, metaInfoClient}
}

// Stat returns TorrentInfo for given file name. Returns error if the file does
// not exist.
func (a *AgentTorrentArchive) Stat(name string) (*TorrentInfo, error) {
	downloadOrCache := a.fs.States().Download().Cache()

	raw, err := downloadOrCache.GetMetadata(name, store.NewTorrentMeta())
	if err != nil {
		return nil, fmt.Errorf("get metainfo metadata: %s", err)
	}
	mi, err := torlib.DeserializeMetaInfo(raw)
	if err != nil {
		return nil, fmt.Errorf("deserialize metainfo: %s", err)
	}

	raw, err = downloadOrCache.GetMetadata(name, store.NewPieceStatus())
	if err != nil {
		return nil, fmt.Errorf("get piece status metadata: %s", err)
	}
	b := newBitfieldFromPieceStatusBytes(name, raw)

	return newTorrentInfo(mi, b), nil
}

// GetTorrent returns a Torrent for an existing file on disk, or initializes the
// file if not present.
func (a *AgentTorrentArchive) GetTorrent(name string) (Torrent, error) {
	downloadOrCache := a.fs.States().Download().Cache()

	miRaw, err := downloadOrCache.GetMetadata(name, store.NewTorrentMeta())
	if os.IsNotExist(err) {
		mi, err := a.metaInfoClient.Download(name)
		if err != nil {
			return nil, fmt.Errorf("download metainfo: %s", err)
		}
		// There's a race condition here, but it's "okay"... Basically, we could
		// initialize a download file with metainfo that is rejected by file store,
		// because someone else beats us to it. However, we catch a lucky break
		// because the only piece of metainfo we use is file length -- which digest
		// (i.e. name) is derived from, so it's "okay".
		if err := a.fs.EnsureDownloadOrCacheFilePresent(mi.Name(), mi.Info.Length); err != nil {
			return nil, fmt.Errorf("ensure download/cache file present: %s", err)
		}
		miRaw, err = mi.Serialize()
		if err != nil {
			return nil, fmt.Errorf("serialize downloaded metainfo: %s", err)
		}
		miRaw, err = downloadOrCache.GetOrSetMetadata(name, store.NewTorrentMeta(), miRaw)
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

	t, err := NewLocalTorrent(a.fs, mi)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// DeleteTorrent moves a torrent to the trash.
func (a *AgentTorrentArchive) DeleteTorrent(name string) error {
	return a.fs.MoveDownloadOrCacheFileToTrash(name)
}
