package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/utils/randutil"
)

func TestNewPeerContext(t *testing.T) {
	require := require.New(t)

	p := PeerContextFixture()

	_, err := NewPeerID(p.PeerID.String())
	require.NoError(err)
	require.False(p.Origin)
}

func TestNewOriginPeerContext(t *testing.T) {
	require := require.New(t)

	p := OriginContextFixture()

	_, err := NewPeerID(p.PeerID.String())
	require.NoError(err)
	require.True(p.Origin)
}

func TestNewOriginPeerContextErrors(t *testing.T) {
	t.Run("empty ip", func(t *testing.T) {
		require := require.New(t)

		_, err := NewPeerContext(
			RandomPeerIDFactory, "zone1", "test01-zone1", "", randutil.Port(), false)
		require.Error(err)
	})

	t.Run("zero port", func(t *testing.T) {
		require := require.New(t)

		_, err := NewPeerContext(
			RandomPeerIDFactory, "zone1", "test01-zone1", randutil.IP(), 0, false)
		require.Error(err)
	})

	t.Run("invalid factory", func(t *testing.T) {
		require := require.New(t)

		_, err := NewPeerContext(
			"invalid", "zone1", "test01-zone1", randutil.IP(), randutil.Port(), false)
		require.Error(err)
	})
}
