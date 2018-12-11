package peerhandoutpolicy

import "github.com/uber/kraken/core"

const _defaultPolicy = "default"

// defaultAssignmentPolicy is a NO-OP policy that assigns all peers
// the highest priority.
type defaultAssignmentPolicy struct{}

func newDefaultAssignmentPolicy() assignmentPolicy {
	return &defaultAssignmentPolicy{}
}

func (p *defaultAssignmentPolicy) assignPriority(peer *core.PeerInfo) (int, string) {
	return 0, "default"
}
