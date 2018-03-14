package peerhandoutpolicy

import "code.uber.internal/infra/kraken/core"

// Returns a new slice of the first n peers after applying the `sorter` function.
func sortedPeers(peers []*core.PeerInfo, n int, sort func(core.PeerInfos)) []*core.PeerInfo {
	newPeers := make([]*core.PeerInfo, len(peers))
	copy(newPeers, peers)

	sort(core.PeerInfos(newPeers))

	if n > len(newPeers) {
		return newPeers
	}
	return newPeers[:n]
}

// Calculates a peers priority given a list of priority predicates. The priority
// is defined to be the index of the first true predicate, or len(predicates)
// if no predicates pass (i.e. the worst priority possible).
func calcPriority(predicates []bool) int {
	for i, p := range predicates {
		if p {
			return i
		}
	}
	return len(predicates)
}
