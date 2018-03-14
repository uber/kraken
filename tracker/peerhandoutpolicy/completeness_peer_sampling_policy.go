package peerhandoutpolicy

import (
	"sort"

	"code.uber.internal/infra/kraken/core"
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
	peers []*core.PeerInfo, n int) ([]*core.PeerInfo, error) {

	return sortedPeers(peers, n, func(s core.PeerInfos) {
		sort.Sort(core.PeersByPriority{PeerInfos: s})
		sort.Stable(core.PeersByComplete{PeerInfos: s})
	}), nil
}
