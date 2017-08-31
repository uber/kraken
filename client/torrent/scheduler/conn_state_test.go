package scheduler

import (
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/torlib"
)

func TestBlacklistBackoff(t *testing.T) {
	config := Config{
		MaxOpenConnectionsPerTorrent: 1,
		InitialBlacklistExpiration:   1 * time.Second,
		BlacklistExpirationBackoff:   2,
		MaxBlacklistExpiration:       8 * time.Second,
	}

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

	config := Config{
		MaxOpenConnectionsPerTorrent: 1,
		InitialBlacklistExpiration:   1 * time.Second,
		BlacklistExpirationBackoff:   2,
		MaxBlacklistExpiration:       8 * time.Second,
		ExpiredBlacklistEntryTTL:     5 * time.Minute,
	}

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
