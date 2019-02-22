package connstate

import (
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler/conn"
)

func testState(config Config, clk clock.Clock) *State {
	return New(config, clk, core.PeerIDFixture(), networkevent.NewTestProducer(), zap.NewNop().Sugar())
}

func TestStateBlacklist(t *testing.T) {
	require := require.New(t)

	config := Config{
		BlacklistDuration: 30 * time.Second,
	}
	clk := clock.NewMock()
	s := testState(config, clk)

	p := core.PeerIDFixture()
	h := core.InfoHashFixture()

	require.NoError(s.Blacklist(p, h))
	require.True(s.Blacklisted(p, h))
	require.Error(s.Blacklist(p, h))

	clk.Add(config.BlacklistDuration + 1)

	require.False(s.Blacklisted(p, h))
	require.NoError(s.Blacklist(p, h))
}

func TestStateBlacklistSnapshot(t *testing.T) {
	require := require.New(t)

	config := Config{
		BlacklistDuration: 30 * time.Second,
	}
	clk := clock.NewMock()
	s := testState(config, clk)

	p := core.PeerIDFixture()
	h := core.InfoHashFixture()

	require.NoError(s.Blacklist(p, h))

	expected := []BlacklistedConn{{p, h, config.BlacklistDuration}}
	require.Equal(expected, s.BlacklistSnapshot())
}

func TestStateClearBlacklist(t *testing.T) {
	require := require.New(t)

	s := testState(Config{}, clock.NewMock())

	h := core.InfoHashFixture()

	var peers []core.PeerID
	for i := 0; i < 10; i++ {
		p := core.PeerIDFixture()
		peers = append(peers, p)
		require.NoError(s.Blacklist(p, h))
		require.True(s.Blacklisted(p, h))
	}

	s.ClearBlacklist(h)

	for _, p := range peers {
		require.False(s.Blacklisted(p, h))
	}
}

func TestStateAddPendingPreventsDuplicates(t *testing.T) {
	require := require.New(t)

	s := testState(Config{}, clock.New())

	p := core.PeerIDFixture()
	h := core.InfoHashFixture()

	require.NoError(s.AddPending(p, h, nil))

	require.Equal(ErrConnAlreadyPending, s.AddPending(p, h, nil))
}

func TestStateAddPendingReservesCapacity(t *testing.T) {
	require := require.New(t)

	config := Config{
		MaxOpenConnectionsPerTorrent: 10,
	}
	s := testState(config, clock.New())

	h := core.InfoHashFixture()

	for i := 0; i < config.MaxOpenConnectionsPerTorrent; i++ {
		require.NoError(s.AddPending(core.PeerIDFixture(), h, nil))
	}
	require.Equal(ErrTorrentAtCapacity, s.AddPending(core.PeerIDFixture(), h, nil))
}

func TestStateDeletePendingAllowsFutureAddPending(t *testing.T) {
	require := require.New(t)

	s := testState(Config{}, clock.New())

	p := core.PeerIDFixture()
	h := core.InfoHashFixture()

	require.NoError(s.AddPending(p, h, nil))
	s.DeletePending(p, h)
	require.NoError(s.AddPending(p, h, nil))
}

func TestStateDeletePendingFreesCapacity(t *testing.T) {
	require := require.New(t)

	s := testState(Config{MaxOpenConnectionsPerTorrent: 1}, clock.New())

	h := core.InfoHashFixture()
	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()

	require.NoError(s.AddPending(p1, h, nil))
	require.Equal(ErrTorrentAtCapacity, s.AddPending(p2, h, nil))
	s.DeletePending(p1, h)
	require.NoError(s.AddPending(p2, h, nil))
}

func TestStateMovePendingToActivePreventsFuturePending(t *testing.T) {
	require := require.New(t)

	s := testState(Config{}, clock.New())

	c, cleanup := conn.Fixture()
	defer cleanup()

	require.NoError(s.AddPending(c.PeerID(), c.InfoHash(), nil))
	require.NoError(s.MovePendingToActive(c))
	require.Equal(ErrConnAlreadyActive, s.AddPending(c.PeerID(), c.InfoHash(), nil))
}

func TestStateMovePendingToActiveRejectsNonPendingConns(t *testing.T) {
	require := require.New(t)

	s := testState(Config{}, clock.New())

	c, cleanup := conn.Fixture()
	defer cleanup()

	require.Equal(ErrInvalidActiveTransition, s.MovePendingToActive(c))

	require.NoError(s.AddPending(c.PeerID(), c.InfoHash(), nil))
	require.NoError(s.MovePendingToActive(c))
	require.Equal(ErrInvalidActiveTransition, s.MovePendingToActive(c))
}

func TestStateMovePendingToActiveRejectsClosedConns(t *testing.T) {
	require := require.New(t)

	s := testState(Config{}, clock.New())

	c, cleanup := conn.Fixture()
	defer cleanup()

	require.NoError(s.AddPending(c.PeerID(), c.InfoHash(), nil))
	c.Close()
	require.Equal(ErrConnClosed, s.MovePendingToActive(c))
}

func TestStateDeleteActiveFreesCapacity(t *testing.T) {
	require := require.New(t)

	s := testState(Config{MaxOpenConnectionsPerTorrent: 1}, clock.New())

	c, cleanup := conn.Fixture()
	defer cleanup()

	p2 := core.PeerIDFixture()

	require.NoError(s.AddPending(c.PeerID(), c.InfoHash(), nil))
	require.NoError(s.MovePendingToActive(c))
	require.Equal(ErrTorrentAtCapacity, s.AddPending(p2, c.InfoHash(), nil))
	s.DeleteActive(c)
	require.NoError(s.AddPending(p2, c.InfoHash(), nil))
}

func TestStateDeleteActiveNoopsWhenConnIsNotActive(t *testing.T) {
	require := require.New(t)

	s := testState(Config{MaxOpenConnectionsPerTorrent: 1}, clock.New())

	c, cleanup := conn.Fixture()
	defer cleanup()

	require.NoError(s.AddPending(core.PeerIDFixture(), c.InfoHash(), nil))

	s.DeleteActive(c)

	require.Equal(ErrTorrentAtCapacity, s.AddPending(core.PeerIDFixture(), c.InfoHash(), nil))
}

func TestStateActiveConns(t *testing.T) {
	require := require.New(t)

	s := testState(Config{}, clock.New())

	conns := make(map[core.PeerID]*conn.Conn)
	for i := 0; i < 10; i++ {
		c, cleanup := conn.Fixture()
		defer cleanup()

		conns[c.PeerID()] = c

		require.NoError(s.AddPending(c.PeerID(), c.InfoHash(), nil))
		require.NoError(s.MovePendingToActive(c))
	}

	result := s.ActiveConns()
	require.Len(result, len(conns))
	for _, c := range result {
		require.Equal(conns[c.PeerID()], c)
	}

	for _, c := range conns {
		s.DeleteActive(c)
	}
	require.Empty(s.ActiveConns())
}

func TestMaxMutualConns(t *testing.T) {
	require := require.New(t)

	mutualConnLimit := 5
	s := testState(Config{
		MaxMutualConnections: mutualConnLimit, MaxOpenConnectionsPerTorrent: 20}, clock.New())

	neighbors := make([]core.PeerID, 10)
	h := core.InfoHashFixture()
	for i := 0; i < 10; i++ {
		peerID := core.PeerIDFixture()
		neighbors[i] = peerID
		require.NoError(s.AddPending(peerID, h, nil))
	}
	require.Equal(s.AddPending(core.PeerIDFixture(), h, neighbors), ErrTooManyMutualConns)
	require.Equal(s.AddPending(core.PeerIDFixture(), h, neighbors[:mutualConnLimit+1]), ErrTooManyMutualConns)
	require.NoError(s.AddPending(core.PeerIDFixture(), h, neighbors[:mutualConnLimit]))
}
