package peerhandoutpolicy

import (
	"testing"

	"code.uber.internal/infra/kraken/core"

	"github.com/stretchr/testify/require"
)

func TestPriorityPolicyRemoveSource(t *testing.T) {
	require := require.New(t)

	policy := DefaultPriorityPolicyFixture()

	src := core.PeerInfoFixture()
	peers := make([]*core.PeerInfo, 10)
	for k := 0; k < len(peers); k++ {
		peers[k] = core.PeerInfoFixture()
	}
	peers = append(peers, src)

	sorted := policy.SortPeers(src, peers)
	require.Len(sorted, len(peers)-1)
	for k := 0; k < len(sorted); k++ {
		require.NotEqual(src, sorted[k])
	}
}
