package peerhandoutpolicy

import (
	"sort"

	"code.uber.internal/infra/kraken/torlib"
)

// CompletenessPeerSamplingPolicy selects peers first on download completeness,
// then on priority.
type CompletenessPeerSamplingPolicy struct{}

// NewCompletenessPeerSamplingPolicy is used as a PeerSamplingPolicy factory.
func NewCompletenessPeerSamplingPolicy() PeerSamplingPolicy {
	return &CompletenessPeerSamplingPolicy{}
}

// SamplePeers returns the top n peers, ordered first on download completeness, then
// on priority.
func (p *CompletenessPeerSamplingPolicy) SamplePeers(
	peers []*torlib.PeerInfo, n int) ([]*torlib.PeerInfo, error) {

	return sortedPeers(peers, n, func(s peerInfos) {
		sort.Sort(byPriority{s})
		sort.Stable(sort.Reverse(byBytesDownloaded{s}))
	}), nil
}
