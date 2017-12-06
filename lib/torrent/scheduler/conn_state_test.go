package scheduler

import (
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/memsize"
)

func TestBlacklistBackoff(t *testing.T) {
	config := connStateConfigFixture()
	config.MaxOpenConnectionsPerTorrent = 1
	config.InitialBlacklistExpiration = 30 * time.Second
	config.BlacklistExpirationBackoff = 2
	config.MaxBlacklistExpiration = 5 * time.Minute

	for _, test := range []struct {
		description string
		failures    int
		expected    time.Duration
	}{
		{"first failure", 1, 30 * time.Second},
		{"second failure", 2, time.Minute},
		{"third failure", 3, 2 * time.Minute},
		{"fourth failure", 4, 4 * time.Minute},
		{"maxes out after fourth failure", 10, 5 * time.Minute},
	} {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)

			clk := clock.NewMock()
			clk.Set(time.Now())

			s := newConnState(torlib.PeerIDFixture(), config, clk, networkevent.NewTestProducer())

			peerID := torlib.PeerIDFixture()
			infoHash := torlib.InfoHashFixture()

			var remaining time.Duration
			for i := 0; i < test.failures; i++ {
				require.NoError(s.Blacklist(peerID, infoHash))

				err := s.AddPending(peerID, infoHash)
				require.Error(err)
				blacklistErr, ok := err.(blacklistError)
				require.True(ok)
				remaining = blacklistErr.remaining

				clk.Add(remaining)

				// Peer/hash should no longer be blacklisted.
				require.NoError(s.AddPending(peerID, infoHash))

				s.DeletePending(peerID, infoHash)
			}

			// Checks the remaining backoff of the final iteration.
			require.Equal(test.expected, remaining)
		})
	}
}

func TestDeleteStaleBlacklistEntries(t *testing.T) {
	require := require.New(t)

	config := connStateConfigFixture()

	clk := clock.NewMock()
	clk.Set(time.Now())

	s := newConnState(torlib.PeerIDFixture(), config, clk, networkevent.NewTestProducer())

	peerID := torlib.PeerIDFixture()
	infoHash := torlib.InfoHashFixture()

	require.NoError(s.Blacklist(peerID, infoHash))

	err := s.AddPending(peerID, infoHash)
	require.Error(err)
	require.Equal(config.InitialBlacklistExpiration, err.(blacklistError).remaining)

	// Fast-forward to when connection has expired from blacklist.
	clk.Add(config.InitialBlacklistExpiration)

	clk.Add(config.ExpiredBlacklistEntryTTL)

	s.DeleteStaleBlacklistEntries()

	require.NoError(s.Blacklist(peerID, infoHash))

	// Backoff should have been reset.
	err = s.AddPending(peerID, infoHash)
	require.Error(err)
	require.Equal(config.InitialBlacklistExpiration, err.(blacklistError).remaining)
}

func transitionToActive(t *testing.T, s *connState, c *conn) {
	require.NoError(t, s.AddPending(c.PeerID, c.InfoHash))
	require.NoError(t, s.MovePendingToActive(c))
}

func TestChangesToActiveConnsRedistributesBandwidth(t *testing.T) {
	require := require.New(t)

	config := connStateConfigFixture()

	s := newConnState(torlib.PeerIDFixture(), config, clock.New(), networkevent.NewTestProducer())

	torrent, cleanup := storage.TorrentFixture(128, 32)
	defer cleanup()

	c1, _, cleanup := connFixture(connConfigFixture(), torrent)
	defer cleanup()

	c2, _, cleanup := connFixture(connConfigFixture(), torrent)
	defer cleanup()

	// First conn takes all bandwidth.
	transitionToActive(t, s, c1)
	require.Equal(c1.GetEgressBandwidthLimit(), config.MaxGlobalEgressBytesPerSec)

	// Adding second conn splits bandwidth with first conn.
	transitionToActive(t, s, c2)
	require.Equal(c1.GetEgressBandwidthLimit(), config.MaxGlobalEgressBytesPerSec/2)
	require.Equal(c2.GetEgressBandwidthLimit(), config.MaxGlobalEgressBytesPerSec/2)

	// Removing first conn gives all bandwidth to second conn.
	s.DeleteActive(c1)
	require.Equal(c2.GetEgressBandwidthLimit(), config.MaxGlobalEgressBytesPerSec)

	// Removing all conns to hit no-op case.
	s.DeleteActive(c2)
}

func TestAddingActiveConnsNeverRedistributesBandwidthBelowMin(t *testing.T) {
	require := require.New(t)

	config := connStateConfigFixture()
	config.MaxGlobalEgressBytesPerSec = 4 * memsize.KB
	config.MinConnEgressBytesPerSec = memsize.KB

	s := newConnState(torlib.PeerIDFixture(), config, clock.New(), networkevent.NewTestProducer())

	torrent, cleanup := storage.TorrentFixture(128, 32)
	defer cleanup()

	// After adding 4 active conns, the bandwidth should hit the min.
	for i := 0; i < 12; i++ {
		c, _, cleanup := connFixture(connConfigFixture(), torrent)
		defer cleanup()
		transitionToActive(t, s, c)
	}
	for _, c := range s.ActiveConns() {
		require.Equal(c.GetEgressBandwidthLimit(), config.MinConnEgressBytesPerSec)
	}
}

func TestConnStateBlacklistSnapshot(t *testing.T) {
	require := require.New(t)

	config := connStateConfigFixture()
	clk := clock.NewMock()

	s := newConnState(torlib.PeerIDFixture(), config, clk, networkevent.NewTestProducer())

	pid := torlib.PeerIDFixture()
	h := torlib.InfoHashFixture()

	require.NoError(s.AddPending(pid, h))
	require.NoError(s.Blacklist(pid, h))

	expected := []BlacklistedConn{{pid, h, config.InitialBlacklistExpiration}}
	require.Equal(expected, s.BlacklistSnapshot())
}
