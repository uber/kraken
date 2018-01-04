package storage

import (
	"errors"
	"fmt"

	"code.uber.internal/go-common.git/x/mysql"

	"code.uber.internal/infra/kraken/torlib"
)

// Storage errors.
var (
	ErrExists   = errors.New("record already exists")
	ErrNotFound = errors.New("not found")
)

// PeerStore provides storage for announcing peers.
type PeerStore interface {

	// GetPeers returns all peers announcing for infohash.
	GetPeers(infohash string) ([]*torlib.PeerInfo, error)

	// UpdatePeer updates peer fields.
	UpdatePeer(peer *torlib.PeerInfo) error

	// GetOrigins returns all origin peers serving infohash.
	GetOrigins(infohash string) ([]*torlib.PeerInfo, error)

	// UpdateOrigins overwrites all origin peers serving infohash.
	UpdateOrigins(infohash string, origins []*torlib.PeerInfo) error
}

// MetaInfoStore provides storage for torrent metainfo.
type MetaInfoStore interface {

	// GetMetaInfo returns torrent's metainfo as raw string. Should return
	// ErrNotFound if the name is not found.
	GetMetaInfo(name string) ([]byte, error)

	// SetMetaInfo sets torrent in tracker's storage given metainfo.
	// Should return ErrExists if there already exists a metainfo
	// for the filename.
	SetMetaInfo(mi *torlib.MetaInfo) error
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
	MetaInfoStore
	ManifestStore
}

// StoreProvider provides constructors for datastores. Ensures that at most one
// storage backend is created regardless of how many stores it backs.
type StoreProvider struct {
	config Config
	nemo   mysql.Configuration

	// Caches previously created storage backends.
	mysqlStorage *MySQLStorage
	redisStorage *RedisStorage
}

// NewStoreProvider creates a new StoreProvider.
func NewStoreProvider(config Config, nemo mysql.Configuration) *StoreProvider {
	return &StoreProvider{config: config, nemo: nemo}
}

// GetPeerStore returns the configured PeerStore.
func (p *StoreProvider) GetPeerStore() (PeerStore, error) {
	s, err := p.getStorageBackend(p.config.PeerStore)
	if err != nil {
		return nil, err
	}
	ps, ok := s.(PeerStore)
	if !ok {
		return nil, fmt.Errorf("PeerStore not supported for %s", p.config.PeerStore)
	}
	return ps, nil
}

// GetMetaInfoStore returns the configured MetaInfoStore.
func (p *StoreProvider) GetMetaInfoStore() (MetaInfoStore, error) {
	s, err := p.getStorageBackend(p.config.MetaInfoStore)
	if err != nil {
		return nil, err
	}
	ts, ok := s.(MetaInfoStore)
	if !ok {
		return nil, fmt.Errorf("MetaInfoStore not supported for %s", p.config.MetaInfoStore)
	}
	return ts, nil
}

// GetManifestStore returns the configured ManifestStore.
func (p *StoreProvider) GetManifestStore() (ManifestStore, error) {
	s, err := p.getStorageBackend(p.config.ManifestStore)
	if err != nil {
		return nil, err
	}
	ms, ok := s.(ManifestStore)
	if !ok {
		return nil, fmt.Errorf("ManifestStore not supported for %s", p.config.ManifestStore)
	}
	return ms, nil
}

func (p *StoreProvider) getStorageBackend(name string) (interface{}, error) {
	switch name {
	case "mysql":
		if p.mysqlStorage == nil {
			s, err := NewMySQLStorage(p.nemo, p.config.MySQL)
			if err != nil {
				return nil, fmt.Errorf("mysql storage initialization failed: %s", err)
			}
			if err := s.RunMigration(); err != nil {
				return nil, fmt.Errorf("mysql migration failed: %s", err)
			}
			p.mysqlStorage = s
		}
		return p.mysqlStorage, nil
	case "redis":
		if p.redisStorage == nil {
			s, err := NewRedisStorage(p.config.Redis)
			if err != nil {
				return nil, fmt.Errorf("redis storage initialization failed: %s", err)
			}
			p.redisStorage = s
		}
		return p.redisStorage, nil
	default:
		return nil, fmt.Errorf("invalid storage backend: %q", name)
	}
}
