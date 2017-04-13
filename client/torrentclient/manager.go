package torrentclient

import (
	"code.uber.internal/infra/kraken-torrent/metainfo"
	"code.uber.internal/infra/kraken-torrent/storage"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"
)

const perm = 0755

// Manager implements a data storage for torrent. It should be initiated only once at the start of the program
type Manager struct {
	config *configuration.Config
	store  *store.LocalFileStore
}

// NewManager returns a new Manager
func NewManager(config *configuration.Config, store *store.LocalFileStore) *Manager {
	return &Manager{
		config: config,
		store:  store,
	}
}

// OpenTorrent returns torrent specified by the info
func (m *Manager) OpenTorrent(info *metainfo.Info, infoHash metainfo.Hash) (storage.TorrentImpl, error) {
	// new torrent, create new LayerStore
	torrent := NewTorrent(m.config, m.store, info.Name, info.Length, info.NumPieces())
	err := torrent.Open()
	if err != nil {
		return nil, err
	}

	return torrent, nil
}

// Close closes the storage
func (m *Manager) Close() error {
	return nil
}
