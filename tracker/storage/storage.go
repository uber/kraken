package storage

import (
	"errors"
	"fmt"

	"github.com/andres-erbsen/clock"

	"code.uber.internal/infra/kraken/core"
)

// Storage errors.
var (
	ErrExists   = errors.New("record already exists")
	ErrNotFound = errors.New("not found")
)

// PeerStore provides storage for announcing peers.
type PeerStore interface {

	// GetPeers returns at most n random peers announcing for h.
	GetPeers(h core.InfoHash, n int) ([]*core.PeerInfo, error)

	// UpdatePeer updates peer fields.
	UpdatePeer(h core.InfoHash, peer *core.PeerInfo) error

	// GetOrigins returns all origin peers serving h.
	GetOrigins(h core.InfoHash) ([]*core.PeerInfo, error)

	// UpdateOrigins overwrites all origin peers serving h.
	UpdateOrigins(h core.InfoHash, origins []*core.PeerInfo) error
}

// MetaInfoStore provides storage for torrent metainfo.
type MetaInfoStore interface {

	// GetMetaInfo returns torrent's metainfo as raw string. Should return
	// ErrNotFound if the name is not found.
	GetMetaInfo(name string) ([]byte, error)

	// SetMetaInfo sets torrent in tracker's storage given metainfo.
	// Should return ErrExists if there already exists a metainfo
	// for the filename.
	SetMetaInfo(mi *core.MetaInfo) error
}

// Storage provides a combined interface for all stores. Useful for mocking.
// TODO(codyg): Replace all "storage" variables names with "store".
type Storage interface {
	PeerStore
	MetaInfoStore
}

// StoreProvider provides constructors for datastores. Ensures that at most one
// storage backend is created regardless of how many stores it backs.
type StoreProvider struct {
	config Config

	// Caches previously created storage backends.
	redisStorage *RedisStorage
}

// NewStoreProvider creates a new StoreProvider.
func NewStoreProvider(config Config) *StoreProvider {
	return &StoreProvider{config: config}
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

func (p *StoreProvider) getStorageBackend(name string) (interface{}, error) {
	switch name {
	case "redis":
		if p.redisStorage == nil {
			s, err := NewRedisStorage(p.config.Redis, clock.New())
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
