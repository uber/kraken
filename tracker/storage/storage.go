package storage

import (
	"fmt"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/torlib"

	_ "github.com/go-sql-driver/mysql" // need this side effect import for mysql
)

func init() {
	register("mysql", new(mysqlStorageFactory))
	register("redis", new(redisStorageFactory))
}

// PeerStore provides storage for announcing peers.
type PeerStore interface {

	// GetPeers returns all peers announcing for infohash.
	GetPeers(infohash string) ([]*torlib.PeerInfo, error)

	// UpdatePeer updates peer fields.
	UpdatePeer(peer *torlib.PeerInfo) error
}

// TorrentStore provides storage for torrent metainfo.
type TorrentStore interface {

	// GetTorrent returns torrent's metainfo as raw string.
	GetTorrent(name string) (string, error)

	// CreateTorrent creates torrent in tracker's storage given metainfo.
	CreateTorrent(meta *torlib.MetaInfo) error
}

// ManifestStore provides storage for Docker image manifests.
type ManifestStore interface {

	// GetManifest returns stored manifest as raw string given tag.
	GetManifest(tag string) (string, error)

	// CreateManifest creates manfist given tag and manifest.
	CreateManifest(tag, manifestRaw string) error

	// DeleteManifest deletes manifest from tracker given tag.
	DeleteManifest(tag string) error
}

// Storage provides a combined interface for all stores. Useful for mocking.
// TODO(codyg): Replace all "storage" variables names with "store".
type Storage interface {
	PeerStore
	TorrentStore
	ManifestStore
}

// GetPeerStore returns the configured PeerStore.
func GetPeerStore(cfg config.DatabaseConfig) (PeerStore, error) {
	s, err := getStore(cfg.PeerStore, cfg)
	if err != nil {
		return nil, err
	}
	ps, ok := s.(PeerStore)
	if !ok {
		return nil, fmt.Errorf("PeerStore not supported for %s", cfg.PeerStore)
	}
	return ps, nil
}

// GetTorrentStore returns the configured TorrentStore.
func GetTorrentStore(cfg config.DatabaseConfig) (TorrentStore, error) {
	s, err := getStore(cfg.TorrentStore, cfg)
	if err != nil {
		return nil, err
	}
	ts, ok := s.(TorrentStore)
	if !ok {
		return nil, fmt.Errorf("TorrentStore not supported for %s", cfg.TorrentStore)
	}
	return ts, nil
}

// GetManifestStore returns the configured ManifestStore.
func GetManifestStore(cfg config.DatabaseConfig) (ManifestStore, error) {
	s, err := getStore(cfg.ManifestStore, cfg)
	if err != nil {
		return nil, err
	}
	ms, ok := s.(ManifestStore)
	if !ok {
		return nil, fmt.Errorf("ManifestStore not supported for %s", cfg.ManifestStore)
	}
	return ms, nil
}

type storeFactory interface {
	GetStore(config.DatabaseConfig) (interface{}, error)
}

var _storeFactories = make(map[string]storeFactory)

func register(name string, f storeFactory) {
	if f == nil {
		log.Panicf("No factory supplied for %s", name)
	}
	if _, ok := _storeFactories[name]; ok {
		log.Panicf("Duplicate factory registered for %s", name)
	}
	_storeFactories[name] = f
}

type redisStorageFactory struct {
	store *RedisStorage
}

func (f *redisStorageFactory) GetStore(cfg config.DatabaseConfig) (interface{}, error) {
	var err error
	if f.store == nil {
		f.store, err = NewRedisStorage(cfg.Redis)
	}
	return f.store, err
}

type mysqlStorageFactory struct {
	store *MySQLStorage
}

func (f *mysqlStorageFactory) GetStore(cfg config.DatabaseConfig) (interface{}, error) {
	var err error
	if f.store == nil {
		f.store, err = NewMySQLStorage(cfg.MySQL)
	}
	return f.store, err
}

func getStore(name string, cfg config.DatabaseConfig) (interface{}, error) {
	f, ok := _storeFactories[name]
	if !ok {
		return nil, fmt.Errorf("store not found: %s", name)
	}
	s, err := f.GetStore(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize store: %s", err)
	}
	return s, nil
}
