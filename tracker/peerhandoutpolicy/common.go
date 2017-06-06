package peerhandoutpolicy

import "code.uber.internal/infra/kraken/tracker/storage"

type peerInfos []*storage.PeerInfo

func (s peerInfos) Len() int      { return len(s) }
func (s peerInfos) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type byPriority struct{ peerInfos }

func (s byPriority) Less(i, j int) bool {
	return s.peerInfos[i].Priority < s.peerInfos[j].Priority
}

type byBytesDownloaded struct{ peerInfos }

func (s byBytesDownloaded) Less(i, j int) bool {
	return s.peerInfos[i].BytesDownloaded < s.peerInfos[j].BytesDownloaded
}

// Returns a new slice of the first n peers after applying the `sorter` function.
func sortedPeers(peers []*storage.PeerInfo, n int, sort func(peerInfos)) []*storage.PeerInfo {
	newPeers := make([]*storage.PeerInfo, len(peers))
	copy(newPeers, peers)

	sort(peerInfos(newPeers))

	if n > len(newPeers) {
		return newPeers
	}
	return newPeers[:n]
}
