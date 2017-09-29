package storage

import (
	"sort"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/torlib"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/require"
)

func flushdb(config RedisConfig) {
	c, err := redis.Dial("tcp", config.Addr)
	if err != nil {
		panic(err)
	}
	if _, err := c.Do("FLUSHDB"); err != nil {
		panic(err)
	}
}

func sortedPeerIDs(peers []*torlib.PeerInfo) []string {
	pids := make([]string, len(peers))
	for i := range pids {
		pids[i] = peers[i].PeerID
	}
	sort.Strings(pids)
	return pids
}

func TestRedisStorageGetPeersOnlyReturnsTaggedFields(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	p := torlib.PeerInfoFixture()

	require.NoError(s.UpdatePeer(p))

	peers, err := s.GetPeers(p.InfoHash)
	require.NoError(err)
	require.Equal(peers, []*torlib.PeerInfo{{
		InfoHash:        p.InfoHash,
		PeerID:          p.PeerID,
		IP:              p.IP,
		Port:            p.Port,
		DC:              p.DC,
		BytesDownloaded: p.BytesDownloaded,
		Flags:           p.Flags,
	}})
}

func TestRedisStorageGetPeersFromMultipleWindows(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()
	config.PeerSetWindowSizeSecs = 10
	config.MaxPeerSetWindows = 3

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	now := time.Now()
	s.now = func() time.Time { return now }
	// Reset time to the beginning of a window.
	now = time.Unix(s.curPeerSetWindow(), 0)

	mi := torlib.MetaInfoFixture()

	// Each peer will be added on a different second to distribute them across
	// multiple windows.
	peers := make([]*torlib.PeerInfo, config.PeerSetWindowSizeSecs*config.MaxPeerSetWindows)

	for i := range peers {
		if i > 0 {
			// Fast-forward clock.
			now = now.Add(time.Second)
		}

		p := torlib.PeerInfoForMetaInfoFixture(mi)
		peers[i] = p

		require.NoError(s.UpdatePeer(p))
	}

	result, err := s.GetPeers(mi.InfoHash.String())
	require.NoError(err)
	require.Equal(sortedPeerIDs(peers), sortedPeerIDs(result))
}

func TestRedisStoragePeerExpiration(t *testing.T) {
	t.Skip("Flaky test: needs investigation")

	require := require.New(t)

	config := redisConfigFixture()
	config.PeerSetWindowSizeSecs = 1
	config.MaxPeerSetWindows = 2

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	p := torlib.PeerInfoFixture()

	require.NoError(s.UpdatePeer(p))

	result, err := s.GetPeers(p.InfoHash)
	require.NoError(err)
	require.Len(result, 1)

	time.Sleep(1500 * time.Millisecond)

	result, err = s.GetPeers(p.InfoHash)
	require.NoError(err)
	require.Empty(result)
}

func TestRedisStorageCreateAndGetTorrent(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	mi := torlib.MetaInfoFixture()

	require.NoError(s.CreateTorrent(mi))

	result, err := s.GetTorrent(mi.Name())
	require.NoError(err)

	expected, err := mi.Serialize()
	require.NoError(err)

	require.Equal(expected, result)
}
