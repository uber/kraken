package scheduler

import (
	"testing"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/utils/memsize"
)

func transitionToActive(t *testing.T, s *connState, c *conn.Conn) {
	require.NoError(t, s.AddPending(c.PeerID(), c.InfoHash()))
	require.NoError(t, s.MovePendingToActive(c))
}

func TestChangesToActiveConnsRedistributesBandwidth(t *testing.T) {
	require := require.New(t)

	config := connStateConfigFixture()

	s := newConnState(core.PeerIDFixture(), config, clock.New(), networkevent.NewTestProducer())

	info, cleanup := storage.TorrentInfoFixture(128, 32)
	defer cleanup()

	c1, cleanup := conn.Fixture(conn.ConfigFixture(), info)
	defer cleanup()

	c2, cleanup := conn.Fixture(conn.ConfigFixture(), info)
	defer cleanup()

	// First conn takes all bandwidth.
	transitionToActive(t, s, c1)
	require.Equal(c1.GetEgressBandwidthLimit(), config.MaxGlobalEgressBitsPerSec)

	// Adding second conn splits bandwidth with first conn.
	transitionToActive(t, s, c2)
	require.Equal(c1.GetEgressBandwidthLimit(), config.MaxGlobalEgressBitsPerSec/2)
	require.Equal(c2.GetEgressBandwidthLimit(), config.MaxGlobalEgressBitsPerSec/2)

	// Removing first conn gives all bandwidth to second conn.
	s.DeleteActive(c1)
	require.Equal(c2.GetEgressBandwidthLimit(), config.MaxGlobalEgressBitsPerSec)

	// Removing all conns to hit no-op case.
	s.DeleteActive(c2)
}

func TestAddingActiveConnsNeverRedistributesBandwidthBelowMin(t *testing.T) {
	require := require.New(t)

	config := connStateConfigFixture()
	config.MaxGlobalEgressBitsPerSec = 4 * memsize.Kbit
	config.MinConnEgressBitsPerSec = memsize.Kbit

	s := newConnState(core.PeerIDFixture(), config, clock.New(), networkevent.NewTestProducer())

	info, cleanup := storage.TorrentInfoFixture(128, 32)
	defer cleanup()

	// After adding 4 active conns, the bandwidth should hit the min.
	for i := 0; i < 12; i++ {
		c, cleanup := conn.Fixture(conn.ConfigFixture(), info)
		defer cleanup()
		transitionToActive(t, s, c)
	}
	for _, c := range s.ActiveConns() {
		require.Equal(c.GetEgressBandwidthLimit(), config.MinConnEgressBitsPerSec)
	}
}

func TestConnStateBlacklist(t *testing.T) {
	require := require.New(t)

	config := connStateConfigFixture()
	clk := clock.NewMock()

	s := newConnState(core.PeerIDFixture(), config, clk, networkevent.NewTestProducer())

	pid := core.PeerIDFixture()
	h := core.InfoHashFixture()

	require.NoError(s.Blacklist(pid, h))
	require.True(s.Blacklisted(pid, h))
	require.Error(s.Blacklist(pid, h))

	clk.Add(config.BlacklistDuration + 1)

	require.False(s.Blacklisted(pid, h))
	require.NoError(s.Blacklist(pid, h))
}

func TestConnStateBlacklistSnapshot(t *testing.T) {
	require := require.New(t)

	config := connStateConfigFixture()
	clk := clock.NewMock()

	s := newConnState(core.PeerIDFixture(), config, clk, networkevent.NewTestProducer())

	pid := core.PeerIDFixture()
	h := core.InfoHashFixture()

	require.NoError(s.Blacklist(pid, h))

	expected := []BlacklistedConn{{pid, h, config.BlacklistDuration}}
	require.Equal(expected, s.BlacklistSnapshot())
}
