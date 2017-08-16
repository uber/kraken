package peerhandoutpolicy

import (
	"sort"

	"code.uber.internal/infra/kraken/torlib"
)

// DefaultPeerSamplingPolicy simply selects peers on priority.
type DefaultPeerSamplingPolicy struct{}

// NewDefaultPeerSamplingPolicy is used as a PeerSamplingPolicy factory.
func NewDefaultPeerSamplingPolicy() PeerSamplingPolicy {
	return &DefaultPeerSamplingPolicy{}
}

// SamplePeers returns a sorted slice of the top n highest priority peers.
func (p *DefaultPeerSamplingPolicy) SamplePeers(
	peers []*torlib.PeerInfo, n int) ([]*torlib.PeerInfo, error) {

	return sortedPeers(peers, n, func(s peerInfos) {
		sort.Sort(byPriority{s})
	}), nil
}
