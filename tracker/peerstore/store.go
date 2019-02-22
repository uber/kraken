package peerstore

import (
	"github.com/uber/kraken/core"
)

// Store provides storage for announcing peers.
type Store interface {

	// GetPeers returns at most n random peers announcing for h.
	GetPeers(h core.InfoHash, n int) ([]*core.PeerInfo, error)

	// UpdatePeer updates peer fields.
	UpdatePeer(h core.InfoHash, peer *core.PeerInfo) error
}
