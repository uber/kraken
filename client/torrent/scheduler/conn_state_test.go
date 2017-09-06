package scheduler

import (
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/memsize"
)

func TestBlacklistBackoff(t *testing.T) {
	config := genConnStateConfig()
	config.MaxOpenConnectionsPerTorrent = 1
	config.InitialBlacklistExpiration = 1 * time.Second
	config.BlacklistExpirationBackoff = 2
	config.MaxBlacklistExpiration = 8 * time.Second

	for _, test := range []struct {
		description string
		failures    int
		expected    time.Duration
	}{
		{"first failure", 1, time.Second},
		{"second failure", 2, 2 * time.Second},
		{"third failure", 3, 4 * time.Second},
		{"fourth failure", 4, 8 * time.Second},
		{"maxes out after fourth failure", 10, 8 * time.Second},
	} {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)

			clk := clock.NewMock()
			clk.Set(time.Now())

			s := newConnState(torlib.PeerIDFixture(), config, clk)

			peerID := torlib.PeerIDFixture()
			infoHash := torlib.InfoHashFixture()

			s.InitCapacity(infoHash)

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

	config := genConnStateConfig()

	clk := clock.NewMock()
	clk.Set(time.Now())

	s := newConnState(torlib.PeerIDFixture(), config, clk)

	peerID := torlib.PeerIDFixture()
	infoHash := torlib.InfoHashFixture()

	s.InitCapacity(infoHash)

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
	s.InitCapacity(c.InfoHash)
	require.NoError(t, s.AddPending(c.PeerID, c.InfoHash))
	require.NoError(t, s.MovePendingToActive(c))
}

func TestChangesToActiveConnsRedistributesBandwidth(t *testing.T) {
	require := require.New(t)

	config := genConnStateConfig()

	s := newConnState(torlib.PeerIDFixture(), config, clock.New())

	c1, cleanup := genTestConn(t, genConnConfig(), 32)
	defer cleanup()

	c2, cleanup := genTestConn(t, genConnConfig(), 32)
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

	config := genConnStateConfig()
	config.MaxGlobalEgressBytesPerSec = 4 * memsize.KB
	config.MinConnEgressBytesPerSec = memsize.KB

	s := newConnState(torlib.PeerIDFixture(), config, clock.New())

	// After adding 4 active conns, the bandwidth should hit the min.
	for i := 0; i < 12; i++ {
		c, cleanup := genTestConn(t, genConnConfig(), 32)
		defer cleanup()
		transitionToActive(t, s, c)
	}
	for _, c := range s.ActiveConns() {
		require.Equal(c.GetEgressBandwidthLimit(), config.MinConnEgressBytesPerSec)
	}
}
