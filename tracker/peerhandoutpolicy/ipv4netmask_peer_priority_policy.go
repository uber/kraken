package peerhandoutpolicy

import (
	"net"

	"code.uber.internal/infra/kraken/core"
)

// IPv4NetmaskPeerPriorityPolicy is Uber specific netmask based peer priority handout policy
type IPv4NetmaskPeerPriorityPolicy struct{}

// NewIPv4NetmaskPeerPriorityPolicy is used as a PeerPriorityPolicy factory.
func NewIPv4NetmaskPeerPriorityPolicy() PeerPriorityPolicy {
	return &IPv4NetmaskPeerPriorityPolicy{}
}

// AssignPeerPriority sets priority based on network topology proximity to a source IP.
func (p *IPv4NetmaskPeerPriorityPolicy) AssignPeerPriority(
	source *core.PeerInfo, peers []*core.PeerInfo) error {

	// Ideally this all needs to be in a clusto, it's just too expensive for
	// now to support without implementing a sensible caching strategy
	// please note currrenty neteng support both /16 and /17 masks per pod
	// so it is possible some amount of peers on
	// different pods could be missclassified as the same pods peers
	src := net.ParseIP(source.IP)

	//local rack mask /24
	localRackMask := net.CIDRMask(24, 32)

	//local pod mask /17
	localPodMask := net.CIDRMask(17, 32)

	for _, peer := range peers {
		dst := net.ParseIP(peer.IP)

		// Sorted in descending order by priority (highest priority = 0).
		predicates := []bool{
			dst.Mask(localRackMask).Equal(src.Mask(localRackMask)),
			dst.Mask(localPodMask).Equal(src.Mask(localPodMask)),
			source.DC == peer.DC,
		}
		peer.Priority = calcPriority(predicates)
		if peer.Origin {
			// Origin peers always have worse priority than regular peers.
			peer.Priority += int64(len(predicates))
		}
	}

	return nil
}
