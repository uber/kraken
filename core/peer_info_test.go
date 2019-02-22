package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortedByPeerID(t *testing.T) {
	require := require.New(t)

	p1 := PeerInfoFixture()
	p2 := PeerInfoFixture()
	p3 := PeerInfoFixture()

	sorted := SortedByPeerID([]*PeerInfo{p1, p2, p3})
	require.True(sorted[0].PeerID.LessThan(sorted[1].PeerID))
	require.True(sorted[1].PeerID.LessThan(sorted[2].PeerID))
}
