package peerhandoutpolicy

import (
	"sort"

	"code.uber.internal/infra/kraken/core"
)

// DefaultPeerSamplingPolicy simply selects peers on priority.
type DefaultPeerSamplingPolicy struct{}

// NewDefaultPeerSamplingPolicy is used as a PeerSamplingPolicy factory.
func NewDefaultPeerSamplingPolicy() PeerSamplingPolicy {
	return &DefaultPeerSamplingPolicy{}
}

// SamplePeers returns a sorted slice of the top n highest priority peers.
func (p *DefaultPeerSamplingPolicy) SamplePeers(
	peers []*core.PeerInfo, n int) ([]*core.PeerInfo, error) {

	return sortedPeers(peers, n, func(s core.PeerInfos) {
		sort.Sort(core.PeersByPriority{PeerInfos: s})
	}), nil
}
