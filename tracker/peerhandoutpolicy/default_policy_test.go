package peerhandoutpolicy

import (
	"testing"

	"github.com/uber/kraken/core"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestDefaultPriorityPolicy(t *testing.T) {
	require := require.New(t)

	policy, err := NewPriorityPolicy(tally.NoopScope, _defaultPolicy)
	require.NoError(err)

	nPeers := 50

	peers := make([]*core.PeerInfo, nPeers)
	for k := 0; k < len(peers); k++ {
		peers[k] = core.PeerInfoFixture()
	}

	policy.SortPeers(core.PeerInfoFixture(), peers)
	require.Len(peers, nPeers)
}
