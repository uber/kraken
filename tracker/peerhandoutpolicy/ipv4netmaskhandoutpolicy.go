package peerhandoutpolicy

import (
	"code.uber.internal/infra/kraken/tracker/storage"
	"net"
	"sort"
)

// IPv4NetmaskPeerHandoutPolicy is Uber specific netmask based peer priority handout policy
type IPv4NetmaskPeerHandoutPolicy struct {
}

// ByPeerPriority is a predicat that supports sorting by priority
type ByPeerPriority []storage.PeerInfo

// Len return length
func (a ByPeerPriority) Len() int { return len(a) }

// Swap swaps two elements
func (a ByPeerPriority) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less is a predicate '<'
func (a ByPeerPriority) Less(i, j int) bool { return a[i].Priority < a[j].Priority }

// GetPeers returns the list of peers ordered
// by network topology proximity to a source IP starting
// with the ip addresses of peers co-located in the same rack
// and followed by groups of the peers in same pods, same datacenter and remaining
// ones. The function mutates input peers
func (cp *IPv4NetmaskPeerHandoutPolicy) GetPeers(
	sourceIP string, sourceDC string, peers []storage.PeerInfo) ([]storage.PeerInfo, error) {

	// Ideally this all needs to be in a clusto, it's just too expensive for
	// now to support without implementing a sensible caching strategy
	// please note currrenty neteng support both /16 and /17 masks per pod
	// so it is possible some amount of peers on
	// different pods could be missclassified as the same pods peers
	src := net.ParseIP(sourceIP)

	//local rack mask /24
	localRackMask := net.CIDRMask(24, 32)

	//local pod mask /17
	localPodMask := net.CIDRMask(17, 32)

	var outputPeers []storage.PeerInfo

	for _, peer := range peers {
		dst := net.ParseIP(peer.IP)

		if dst.Mask(localRackMask).Equal(src.Mask(localRackMask)) {
			peer.Priority = 0
		} else if dst.Mask(localPodMask).Equal(src.Mask(localPodMask)) {
			peer.Priority = 1
		} else if sourceDC == peer.DC {
			peer.Priority = 2
		} else {
			peer.Priority = 3
		}
		outputPeers = append(outputPeers, peer)
	}

	sort.Sort(ByPeerPriority(outputPeers))
	return outputPeers, nil
}
