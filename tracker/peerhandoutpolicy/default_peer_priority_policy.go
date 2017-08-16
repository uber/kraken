package peerhandoutpolicy

import "code.uber.internal/infra/kraken/torlib"

// DefaultPeerPriorityPolicy is a NO-OP policy
type DefaultPeerPriorityPolicy struct{}

// NewDefaultPeerPriorityPolicy is used as a PeerPriorityPolicy factory.
func NewDefaultPeerPriorityPolicy() PeerPriorityPolicy {
	return &DefaultPeerPriorityPolicy{}
}

// AssignPeerPriority assigns all peers to the highest priority.
func (p *DefaultPeerPriorityPolicy) AssignPeerPriority(
	source *torlib.PeerInfo, peers []*torlib.PeerInfo) error {

	for _, peer := range peers {
		peer.Priority = 0
	}
	return nil
}
