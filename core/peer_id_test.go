package core

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/stringset"
)

func TestAddrHashPeerIDFactory(t *testing.T) {
	require := require.New(t)

	ip := randutil.IP()
	port := randutil.Port()
	p1, err := AddrHashPeerIDFactory.GeneratePeerID(ip, port)
	require.NoError(err)
	p2, err := AddrHashPeerIDFactory.GeneratePeerID(ip, port)
	require.NoError(err)
	require.Equal(p1.String(), p2.String())
}

func TestNewPeerIDErrors(t *testing.T) {
	tests := []struct {
		desc  string
		input string
	}{
		{"empty", ""},
		{"invalid hex", "invalid"},
		{"too short", "beef"},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			_, err := NewPeerID(test.input)
			require.Error(t, err)
		})
	}
}

func TestHashedPeerID(t *testing.T) {
	require := require.New(t)

	n := 50

	ids := make(stringset.Set)
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("%s:%d", randutil.IP(), randutil.Port())
		peerID, err := HashedPeerID(addr)
		require.NoError(err)
		ids.Add(peerID.String())
	}

	// None of the hashes should conflict.
	require.Len(ids, n)
}

func TestHashedPeerIDReturnsErrOnEmpty(t *testing.T) {
	require := require.New(t)

	_, err := HashedPeerID("")
	require.Error(err)
}

func TestPeerIDCompare(t *testing.T) {
	require := require.New(t)

	peer1 := PeerIDFixture()
	peer2 := PeerIDFixture()
	if peer1.String() < peer2.String() {
		require.True(peer1.LessThan(peer2))
	} else if peer1.String() > peer2.String() {
		require.True(peer2.LessThan(peer1))
	}
}
