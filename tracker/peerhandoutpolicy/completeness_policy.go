package peerhandoutpolicy

import "code.uber.internal/infra/kraken/core"

const _completenessPolicy = "completeness"

// completenessAssignmentPolicy assigns priorities based on download completeness.
// Peers who've completed downloading are highest, then origins, then other peers.
type completenessAssignmentPolicy struct{}

func newCompletenessAssignmentPolicy() assignmentPolicy {
	return &completenessAssignmentPolicy{}
}

func (p *completenessAssignmentPolicy) assignPriority(peer *core.PeerInfo) (int, string) {
	if peer.Origin {
		return 1, "origin"
	}
	if peer.Complete {
		return 0, "peer_seeder"
	}
	return 2, "peer_incomplete"
}
