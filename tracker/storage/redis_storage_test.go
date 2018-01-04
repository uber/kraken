package storage

import (
	"bytes"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/randutil"
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

func TestRedisStorageGetPeersPopulatesPeerInfoFields(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	p := torlib.PeerInfoFixture()
	p.Complete = true

	require.NoError(s.UpdatePeer(p))

	peers, err := s.GetPeers(p.InfoHash)
	require.NoError(err)
	require.Equal(peers, []*torlib.PeerInfo{{
		InfoHash: p.InfoHash,
		PeerID:   p.PeerID,
		IP:       p.IP,
		Port:     p.Port,
		Complete: true,
	}})
}

func TestRedisStorageGetPeersFromMultipleWindows(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()
	config.PeerSetWindowSize = 10 * time.Second
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
	peers := make([]*torlib.PeerInfo, int(config.PeerSetWindowSize.Seconds())*config.MaxPeerSetWindows)

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
	require.Equal(torlib.SortedPeerIDs(peers), torlib.SortedPeerIDs(result))
}

func TestRedisStorageGetPeersCollapsesCompleteBits(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	p := torlib.PeerInfoFixture()

	require.NoError(s.UpdatePeer(p))

	peers, err := s.GetPeers(p.InfoHash)
	require.NoError(err)
	require.Len(peers, 1)
	require.False(peers[0].Complete)

	p.Complete = true
	require.NoError(s.UpdatePeer(p))

	peers, err = s.GetPeers(p.InfoHash)
	require.NoError(err)
	require.Len(peers, 1)
	require.True(peers[0].Complete)
}

func TestRedisStoragePeerExpiration(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()
	config.PeerSetWindowSize = time.Second
	config.MaxPeerSetWindows = 2

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	p := torlib.PeerInfoFixture()

	require.NoError(s.UpdatePeer(p))

	result, err := s.GetPeers(p.InfoHash)
	require.NoError(err)
	require.Len(result, 1)

	time.Sleep(3 * time.Second)

	result, err = s.GetPeers(p.InfoHash)
	require.NoError(err)
	require.Empty(result)
}

func TestRedisStorageGetOriginsPopulatesPeerInfoFields(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	mi := torlib.MetaInfoFixture()
	infoHash := mi.InfoHash.String()

	origin := torlib.PeerInfoForMetaInfoFixture(mi)
	origin.Complete = true

	require.NoError(s.UpdateOrigins(infoHash, []*torlib.PeerInfo{origin}))

	result, err := s.GetOrigins(infoHash)
	require.NoError(err)
	require.Equal(result, []*torlib.PeerInfo{{
		InfoHash: origin.InfoHash,
		PeerID:   origin.PeerID,
		IP:       origin.IP,
		Port:     origin.Port,
		Complete: true,
		Origin:   true,
	}})
}

func TestRedisStorageUpdateOriginsOverwritesExistingOrigins(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	mi := torlib.MetaInfoFixture()
	infoHash := mi.InfoHash.String()

	initialOrigins := []*torlib.PeerInfo{
		torlib.PeerInfoForMetaInfoFixture(mi),
		torlib.PeerInfoForMetaInfoFixture(mi),
	}

	require.NoError(s.UpdateOrigins(infoHash, initialOrigins))

	result, err := s.GetOrigins(infoHash)
	require.NoError(err)
	require.Equal(torlib.SortedPeerIDs(initialOrigins), torlib.SortedPeerIDs(result))

	newOrigins := []*torlib.PeerInfo{
		torlib.PeerInfoForMetaInfoFixture(mi),
		torlib.PeerInfoForMetaInfoFixture(mi),
		torlib.PeerInfoForMetaInfoFixture(mi),
	}

	require.NoError(s.UpdateOrigins(infoHash, newOrigins))

	result, err = s.GetOrigins(infoHash)
	require.NoError(err)
	require.Equal(torlib.SortedPeerIDs(newOrigins), torlib.SortedPeerIDs(result))
}

func TestRedisStorageOriginsExpiration(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()
	config.OriginsTTL = time.Second

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	mi := torlib.MetaInfoFixture()
	infoHash := mi.InfoHash.String()

	origin := torlib.PeerInfoForMetaInfoFixture(mi)

	require.NoError(s.UpdateOrigins(infoHash, []*torlib.PeerInfo{origin}))

	result, err := s.GetOrigins(infoHash)
	require.NoError(err)
	require.Len(result, 1)

	time.Sleep(2 * time.Second)

	result, err = s.GetOrigins(infoHash)
	require.Equal(err, ErrNoOrigins)
}

func TestRedisStorageSetAndGetMetaInfo(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	mi := torlib.MetaInfoFixture()

	require.NoError(s.SetMetaInfo(mi))

	raw, err := s.GetMetaInfo(mi.Name())
	require.NoError(err)
	result, err := torlib.DeserializeMetaInfo(raw)
	require.NoError(err)
	require.Equal(mi, result)
}

func TestRedisStorageSetMetaInfoConflict(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config)
	require.NoError(err)

	blob := bytes.NewReader(randutil.Blob(32))

	// Two metainfos for same file with different piece lengths.
	mi1, err := torlib.NewMetaInfoFromBlob("some_name", blob, 1)
	require.NoError(err)
	mi2, err := torlib.NewMetaInfoFromBlob("some_name", blob, 2)
	require.NoError(err)

	require.NoError(s.SetMetaInfo(mi1))
	require.Equal(ErrExists, s.SetMetaInfo(mi2))
}
