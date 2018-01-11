package storage

import (
	"errors"
	"fmt"
	"os"
	"time"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/backoff"
)

// AgentTorrentArchiveConfig defines AgentTorrentArchive configuration.
type AgentTorrentArchiveConfig struct {
	DownloadMetaInfoTimeout time.Duration  `yaml:"download_metainfo_timeout"`
	DownloadMetaInfoBackoff backoff.Config `yaml:"download_metainfo_backoff"`
}

func (c AgentTorrentArchiveConfig) applyDefaults() AgentTorrentArchiveConfig {
	if c.DownloadMetaInfoTimeout == 0 {
		c.DownloadMetaInfoTimeout = 10 * time.Minute
	}
	return c
}

// AgentTorrentArchive is a TorrentArchive for agent peers. It is capable
// of initializing torrents in the download directory and serving torrents
// from either the download or cache directory.
type AgentTorrentArchive struct {
	config         AgentTorrentArchiveConfig
	fs             store.FileStore
	metaInfoClient metainfoclient.Client
	backoff        *backoff.Backoff
}

// NewAgentTorrentArchive creates a new AgentTorrentArchive
func NewAgentTorrentArchive(
	config AgentTorrentArchiveConfig,
	fs store.FileStore,
	mic metainfoclient.Client) *AgentTorrentArchive {

	config = config.applyDefaults()

	return &AgentTorrentArchive{config, fs, mic, backoff.New(config.DownloadMetaInfoBackoff)}
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

// CreateTorrent returns a Torrent for either an existing metainfo / file on
// disk, or downloads metainfo and initializes the file. Returns ErrNotFound
// if no metainfo was found.
func (a *AgentTorrentArchive) CreateTorrent(namespace, name string) (Torrent, error) {
	downloadOrCache := a.fs.States().Download().Cache()

	miRaw, err := downloadOrCache.GetMetadata(name, store.NewTorrentMeta())
	if os.IsNotExist(err) {
		mi, err := a.downloadMetaInfo(namespace, name)
		if err != nil {
			if err == metainfoclient.ErrNotFound {
				return nil, ErrNotFound
			}
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
			return nil, fmt.Errorf("get or set metainfo: %s", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("get metainfo: %s", err)
	}
	mi, err := torlib.DeserializeMetaInfo(miRaw)
	if err != nil {
		return nil, fmt.Errorf("parse metainfo: %s", err)
	}

	t, err := NewAgentTorrent(a.fs, mi)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// GetTorrent returns a Torrent for an existing metainfo / file on disk.
func (a *AgentTorrentArchive) GetTorrent(name string) (Torrent, error) {
	downloadOrCache := a.fs.States().Download().Cache()

	miRaw, err := downloadOrCache.GetMetadata(name, store.NewTorrentMeta())
	if err != nil {
		return nil, fmt.Errorf("get metainfo: %s", err)
	}
	mi, err := torlib.DeserializeMetaInfo(miRaw)
	if err != nil {
		return nil, fmt.Errorf("parse metainfo: %s", err)
	}
	t, err := NewAgentTorrent(a.fs, mi)
	if err != nil {
		return nil, fmt.Errorf("initialize torrent: %s", err)
	}
	return t, nil
}

// DeleteTorrent moves a torrent to the trash.
func (a *AgentTorrentArchive) DeleteTorrent(name string) error {
	return a.fs.MoveDownloadOrCacheFileToTrash(name)
}

func (a *AgentTorrentArchive) downloadMetaInfo(namespace string, name string) (*torlib.MetaInfo, error) {
	timer := time.NewTimer(a.config.DownloadMetaInfoTimeout)
	defer timer.Stop()

	var attempt int
	for {
		mi, err := a.metaInfoClient.Download(namespace, name)
		if err != metainfoclient.ErrRetry {
			return mi, err
		}
		select {
		case <-time.After(a.backoff.Duration(attempt)):
		case <-timer.C:
			return nil, errors.New("retries timed out")
		}
		attempt++
	}
}
