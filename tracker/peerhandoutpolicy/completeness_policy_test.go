package peerhandoutpolicy

import (
	"math/rand"
	"testing"

	"github.com/uber/kraken/core"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestCompletenessPriorityPolicy(t *testing.T) {
	require := require.New(t)

	policy, err := NewPriorityPolicy(tally.NoopScope, _completenessPolicy)
	require.NoError(err)

	seeders := 10
	origins := 3
	incomplete := 20

	peers := make([]*core.PeerInfo, seeders+origins+incomplete)
	for k := 0; k < len(peers); k++ {
		p := core.PeerInfoFixture()
		if k < seeders {
			p.Complete = true
		} else if k < origins {
			p.Complete = true
			p.Origin = true
		}
		peers[k] = p
	}

	// shuffle
	for i := 0; i < len(peers); i++ {
		j := rand.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}

	policy.SortPeers(core.PeerInfoFixture(), peers)
	require.Len(peers, seeders+origins+incomplete)
	for k := 0; k < len(peers); k++ {
		p := peers[k]
		if k < seeders {
			require.True(p.Complete)
			require.False(p.Origin)
		} else if k < origins {
			require.True(p.Complete)
			require.True(p.Origin)
		} else {
			require.False(p.Complete)
			require.False(p.Origin)
		}
	}
}
