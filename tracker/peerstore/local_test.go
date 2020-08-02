package peerstore

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/andres-erbsen/clock"
	"github.com/uber/kraken/core"
)

func TestLocalStoreExpiration(t *testing.T) {
	now := time.Date(2019, time.November, 1, 1, 0, 0, 0, time.UTC)
	clk := clock.NewMock()
	clk.Set(now)

	s := NewLocalStore(LocalConfig{TTL: 10 * time.Minute}, clk)

	h1 := core.InfoHashFixture()

	peers, err := s.GetPeers(h1, 0)
	require.NoError(t, err)
	require.Empty(t, peers)

	peers, err = s.GetPeers(h1, 1)
	require.NoError(t, err)
	require.Empty(t, peers)

	p1 := core.PeerInfoFixture()
	require.NoError(t, s.UpdatePeer(h1, p1))

	p2 := core.PeerInfoFixture()
	require.NoError(t, s.UpdatePeer(h1, p2))

	peers, err = s.GetPeers(h1, 2)
	require.NoError(t, err)
	require.ElementsMatch(t, []*core.PeerInfo{p1, p2}, peers)

	peers, err = s.GetPeers(h1, 1)
	require.NoError(t, err)
	require.Len(t, peers, 1)

	clk.Add(5 * time.Minute)

	p3 := core.PeerInfoFixture()
	require.NoError(t, s.UpdatePeer(h1, p3))

	peers, err = s.GetPeers(h1, 3)
	require.NoError(t, err)
	require.ElementsMatch(t, []*core.PeerInfo{p1, p2, p3}, peers)

	clk.Add(5*time.Minute + 1)

	// p1 and p2 are now expired.
	peers, err = s.GetPeers(h1, 3)
	require.NoError(t, err)
	require.ElementsMatch(t, []*core.PeerInfo{p3}, peers)

	clk.Add(5*time.Minute + 1)

	// p3 is now expired.
	peers, err = s.GetPeers(h1, 1)
	require.NoError(t, err)
	require.Empty(t, peers)

	clk.Add(_peerGroupCleanupInterval)

	h2 := core.InfoHashFixture()
	p4 := core.PeerInfoFixture()

	// An arbitrary UpdatePeer call should trigger the cleanup trap.
	// Unfortunately we must reach into the LocalStore's private state
	// to determine whether cleanup actually occurred.
	require.Contains(t, s.peerGroups, h1)
	require.NoError(t, s.UpdatePeer(h2, p4))
	require.NotContains(t, s.peerGroups, h1)

	peers, err = s.GetPeers(h2, 1)
	require.NoError(t, err)
	require.Equal(t, []*core.PeerInfo{p4}, peers)
}

func TestLocalStoreConcurrency(t *testing.T) {
	s := NewLocalStore(LocalConfig{TTL: time.Millisecond}, clock.New())

	hashes := []core.InfoHash{
		core.InfoHashFixture(),
		core.InfoHashFixture(),
		core.InfoHashFixture(),
	}

	// We don't care what the results are, we just want to trigger any race
	// conditions.
	var wg sync.WaitGroup
	for n := 0; n < 1000; n++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for _, h := range hashes {
				require.NoError(t, s.UpdatePeer(h, core.PeerInfoFixture()))
			}
		}()
		go func() {
			defer wg.Done()
			for _, h := range hashes {
				peers, err := s.GetPeers(h, 10)
				require.NoError(t, err)
				require.True(t, len(peers) <= 10)
			}
		}()
	}
	wg.Wait()
}
