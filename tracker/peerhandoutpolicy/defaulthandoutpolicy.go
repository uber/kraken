package peerhandoutpolicy

import (
	"code.uber.internal/infra/kraken/tracker/storage"
)

// DefaultPeerHandoutPolicy is a NO-OP policy
type DefaultPeerHandoutPolicy struct {
}

// GetPeers simply returns all available peers and
// assign them all to a default highest priority
func (cp *DefaultPeerHandoutPolicy) GetPeers(
	sourceIP string, sourceDC string, peers []storage.PeerInfo) ([]storage.PeerInfo, error) {

	// just assign them all the highest priority, if not specified it
	// is supposed to be 0 anyway, just want to be explicit here
	for _, peer := range peers {
		peer.Priority = 0
	}
	return peers, nil
}
