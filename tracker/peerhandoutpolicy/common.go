package peerhandoutpolicy

import "code.uber.internal/infra/kraken/core"

type peerInfos []*core.PeerInfo

func (s peerInfos) Len() int      { return len(s) }
func (s peerInfos) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type byPriority struct{ peerInfos }

func (s byPriority) Less(i, j int) bool {
	return s.peerInfos[i].Priority < s.peerInfos[j].Priority
}

type byComplete struct{ peerInfos }

func (s byComplete) Less(i, j int) bool {
	return s.peerInfos[i].Complete && !s.peerInfos[j].Complete
}

// Returns a new slice of the first n peers after applying the `sorter` function.
func sortedPeers(peers []*core.PeerInfo, n int, sort func(peerInfos)) []*core.PeerInfo {
	newPeers := make([]*core.PeerInfo, len(peers))
	copy(newPeers, peers)

	sort(peerInfos(newPeers))

	if n > len(newPeers) {
		return newPeers
	}
	return newPeers[:n]
}

// Calculates a peers priority given a list of priority predicates. The priority
// is defined to be the index of the first true predicate, or len(predicates)
// if no predicates pass (i.e. the worst priority possible).
func calcPriority(predicates []bool) int64 {
	for i, p := range predicates {
		if p {
			return int64(i)
		}
	}
	return int64(len(predicates))
}
