package storage

import (
	"bytes"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/andres-erbsen/clock"
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

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	p := core.PeerInfoFixture()
	p.Complete = true

	require.NoError(s.UpdatePeer(p))

	peers, err := s.GetPeers(p.InfoHash, 1)
	require.NoError(err)
	require.Equal(peers, []*core.PeerInfo{{
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

	clk := clock.NewMock()
	clk.Set(time.Now())

	s, err := NewRedisStorage(config, clk)
	require.NoError(err)

	// Reset time to the beginning of a window.
	clk.Set(time.Unix(s.curPeerSetWindow(), 0))

	mi := core.MetaInfoFixture()

	// Each peer will be added on a different second to distribute them across
	// multiple windows.
	var peers []*core.PeerInfo
	for i := 0; i < int(config.PeerSetWindowSize.Seconds())*config.MaxPeerSetWindows; i++ {
		if i > 0 {
			clk.Add(time.Second)
		}
		p := core.PeerInfoForMetaInfoFixture(mi)
		peers = append(peers, p)
		require.NoError(s.UpdatePeer(p))
	}

	result, err := s.GetPeers(mi.InfoHash.String(), len(peers))
	require.NoError(err)
	require.Equal(core.SortedPeerIDs(peers), core.SortedPeerIDs(result))
}

func TestRedisStorageGetPeersLimit(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()
	config.PeerSetWindowSize = 10 * time.Second
	config.MaxPeerSetWindows = 3

	flushdb(config)

	clk := clock.NewMock()
	clk.Set(time.Now())

	s, err := NewRedisStorage(config, clk)
	require.NoError(err)

	// Reset time to the beginning of a window.
	clk.Set(time.Unix(s.curPeerSetWindow(), 0))

	mi := core.MetaInfoFixture()

	// Each peer will be added on a different second to distribute them across
	// multiple windows.
	for i := 0; i < 30; i++ {
		if i > 0 {
			clk.Add(time.Second)
		}
		require.NoError(s.UpdatePeer(core.PeerInfoForMetaInfoFixture(mi)))
	}

	// Request more peers than were added on a single window to ensure we obey the limit
	// across multiple windows.
	for i := 0; i < 100; i++ {
		result, err := s.GetPeers(mi.InfoHash.String(), 15)
		require.NoError(err)
		require.Len(result, 15)
	}
}

func TestRedisStorageGetPeersCollapsesCompleteBits(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	p := core.PeerInfoFixture()

	require.NoError(s.UpdatePeer(p))

	peers, err := s.GetPeers(p.InfoHash, 2)
	require.NoError(err)
	require.Len(peers, 1)
	require.False(peers[0].Complete)

	p.Complete = true
	require.NoError(s.UpdatePeer(p))

	peers, err = s.GetPeers(p.InfoHash, 2)
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

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	p := core.PeerInfoFixture()

	require.NoError(s.UpdatePeer(p))

	result, err := s.GetPeers(p.InfoHash, 1)
	require.NoError(err)
	require.Len(result, 1)

	time.Sleep(3 * time.Second)

	result, err = s.GetPeers(p.InfoHash, 1)
	require.NoError(err)
	require.Empty(result)
}

func TestRedisStorageGetOriginsPopulatesPeerInfoFields(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	mi := core.MetaInfoFixture()
	infoHash := mi.InfoHash.String()

	origin := core.PeerInfoForMetaInfoFixture(mi)
	origin.Complete = true

	require.NoError(s.UpdateOrigins(infoHash, []*core.PeerInfo{origin}))

	result, err := s.GetOrigins(infoHash)
	require.NoError(err)
	require.Equal(result, []*core.PeerInfo{{
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

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	mi := core.MetaInfoFixture()
	infoHash := mi.InfoHash.String()

	initialOrigins := []*core.PeerInfo{
		core.PeerInfoForMetaInfoFixture(mi),
		core.PeerInfoForMetaInfoFixture(mi),
	}

	require.NoError(s.UpdateOrigins(infoHash, initialOrigins))

	result, err := s.GetOrigins(infoHash)
	require.NoError(err)
	require.Equal(core.SortedPeerIDs(initialOrigins), core.SortedPeerIDs(result))

	newOrigins := []*core.PeerInfo{
		core.PeerInfoForMetaInfoFixture(mi),
		core.PeerInfoForMetaInfoFixture(mi),
		core.PeerInfoForMetaInfoFixture(mi),
	}

	require.NoError(s.UpdateOrigins(infoHash, newOrigins))

	result, err = s.GetOrigins(infoHash)
	require.NoError(err)
	require.Equal(core.SortedPeerIDs(newOrigins), core.SortedPeerIDs(result))
}

func TestRedisStorageOriginsExpiration(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()
	config.OriginsTTL = time.Second

	flushdb(config)

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	mi := core.MetaInfoFixture()
	infoHash := mi.InfoHash.String()

	origin := core.PeerInfoForMetaInfoFixture(mi)

	require.NoError(s.UpdateOrigins(infoHash, []*core.PeerInfo{origin}))

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

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	mi := core.MetaInfoFixture()

	require.NoError(s.SetMetaInfo(mi))

	raw, err := s.GetMetaInfo(mi.Name())
	require.NoError(err)
	result, err := core.DeserializeMetaInfo(raw)
	require.NoError(err)
	require.Equal(mi, result)
}

func TestRedisStorageSetMetaInfoConflict(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	blob := bytes.NewReader(randutil.Blob(32))

	// Two metainfos for same file with different piece lengths.
	mi1, err := core.NewMetaInfoFromBlob("some_name", blob, 1)
	require.NoError(err)
	mi2, err := core.NewMetaInfoFromBlob("some_name", blob, 2)
	require.NoError(err)

	require.NoError(s.SetMetaInfo(mi1))
	require.Equal(ErrExists, s.SetMetaInfo(mi2))
}
