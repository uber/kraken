package peerstore

import (
	"errors"

	"code.uber.internal/infra/kraken/core"
)

// Store errors.
var (
	ErrNoOrigins = errors.New("no origins found")
)

// Store provides storage for announcing peers.
type Store interface {

	// GetPeers returns at most n random peers announcing for h.
	GetPeers(h core.InfoHash, n int) ([]*core.PeerInfo, error)

	// UpdatePeer updates peer fields.
	UpdatePeer(h core.InfoHash, peer *core.PeerInfo) error

	// GetOrigins returns all origin peers serving h.
	GetOrigins(h core.InfoHash) ([]*core.PeerInfo, error)

	// UpdateOrigins overwrites all origin peers serving h.
	UpdateOrigins(h core.InfoHash, origins []*core.PeerInfo) error
}
