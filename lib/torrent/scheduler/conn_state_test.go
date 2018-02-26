package scheduler

import (
	"testing"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
)

func transitionToActive(t *testing.T, s *connState, c *conn.Conn) {
	require.NoError(t, s.AddPending(c.PeerID(), c.InfoHash()))
	require.NoError(t, s.MovePendingToActive(c))
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
