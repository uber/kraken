package peerhandoutpolicy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/torlib"
)

func TestCompletenessPeerSamplingPolicy(t *testing.T) {
	tests := []struct {
		desc     string
		peers    []*torlib.PeerInfo
		expected []string
	}{
		{
			"completeness first, then priority",
			[]*torlib.PeerInfo{
				{PeerID: "a", Priority: 1, Complete: true},
				{PeerID: "b", Priority: 0, Complete: false},
				{PeerID: "c", Priority: 2, Complete: true},
				{PeerID: "d", Priority: 3, Complete: false},
			},
			[]string{"a", "c", "b", "d"},
		},
		{
			"priority if all complete",
			[]*torlib.PeerInfo{
				{PeerID: "b", Priority: 3, Complete: true},
				{PeerID: "a", Priority: 1, Complete: true},
				{PeerID: "c", Priority: 2, Complete: true},
			},
			[]string{"a", "c", "b"},
		},
		{
			"priority if none complete",
			[]*torlib.PeerInfo{
				{PeerID: "b", Priority: 3, Complete: false},
				{PeerID: "a", Priority: 1, Complete: false},
				{PeerID: "c", Priority: 2, Complete: false},
			},
			[]string{"a", "c", "b"},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			policy := NewCompletenessPeerSamplingPolicy()

			result, err := policy.SamplePeers(test.peers, len(test.peers))
			require.NoError(err)

			var pids []string
			for _, peer := range result {
				pids = append(pids, peer.PeerID)
			}
			require.Equal(test.expected, pids)
		})
	}
}
