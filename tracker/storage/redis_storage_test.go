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

	h := core.InfoHashFixture()

	p := core.PeerInfoFixture()
	p.Complete = true

	require.NoError(s.UpdatePeer(h, p))

	peers, err := s.GetPeers(h, 1)
	require.NoError(err)
	require.Equal(peers, []*core.PeerInfo{p})
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

	h := core.InfoHashFixture()

	// Each peer will be added on a different second to distribute them across
	// multiple windows.
	var peers []*core.PeerInfo
	for i := 0; i < int(config.PeerSetWindowSize.Seconds())*config.MaxPeerSetWindows; i++ {
		if i > 0 {
			clk.Add(time.Second)
		}
		p := core.PeerInfoFixture()
		peers = append(peers, p)
		require.NoError(s.UpdatePeer(h, p))
	}

	result, err := s.GetPeers(h, len(peers))
	require.NoError(err)
	require.Equal(core.SortedByPeerID(peers), core.SortedByPeerID(result))
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

	h := core.InfoHashFixture()

	// Each peer will be added on a different second to distribute them across
	// multiple windows.
	for i := 0; i < 30; i++ {
		if i > 0 {
			clk.Add(time.Second)
		}
		require.NoError(s.UpdatePeer(h, core.PeerInfoFixture()))
	}

	// Request more peers than were added on a single window to ensure we obey the limit
	// across multiple windows.
	for i := 0; i < 100; i++ {
		result, err := s.GetPeers(h, 15)
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

	h := core.InfoHashFixture()
	p := core.PeerInfoFixture()

	require.NoError(s.UpdatePeer(h, p))

	peers, err := s.GetPeers(h, 2)
	require.NoError(err)
	require.Len(peers, 1)
	require.False(peers[0].Complete)

	p.Complete = true
	require.NoError(s.UpdatePeer(h, p))

	peers, err = s.GetPeers(h, 2)
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

	h := core.InfoHashFixture()
	p := core.PeerInfoFixture()

	require.NoError(s.UpdatePeer(h, p))

	result, err := s.GetPeers(h, 1)
	require.NoError(err)
	require.Len(result, 1)

	time.Sleep(3 * time.Second)

	result, err = s.GetPeers(h, 1)
	require.NoError(err)
	require.Empty(result)
}

func TestRedisStorageGetOriginsPopulatesPeerInfoFields(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	h := core.InfoHashFixture()
	origins := []*core.PeerInfo{core.OriginPeerInfoFixture()}

	require.NoError(s.UpdateOrigins(h, origins))

	result, err := s.GetOrigins(h)
	require.NoError(err)
	require.Equal(origins, result)
}

func TestRedisStorageUpdateOriginsOverwritesExistingOrigins(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()

	flushdb(config)

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	h := core.InfoHashFixture()

	initialOrigins := []*core.PeerInfo{
		core.OriginPeerInfoFixture(),
		core.OriginPeerInfoFixture(),
	}

	require.NoError(s.UpdateOrigins(h, initialOrigins))

	result, err := s.GetOrigins(h)
	require.NoError(err)
	require.Equal(core.SortedByPeerID(initialOrigins), core.SortedByPeerID(result))

	newOrigins := []*core.PeerInfo{
		core.OriginPeerInfoFixture(),
		core.OriginPeerInfoFixture(),
		core.OriginPeerInfoFixture(),
	}

	require.NoError(s.UpdateOrigins(h, newOrigins))

	result, err = s.GetOrigins(h)
	require.NoError(err)
	require.Equal(core.SortedByPeerID(newOrigins), core.SortedByPeerID(result))
}

func TestRedisStorageOriginsExpiration(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()
	config.OriginsTTL = time.Second

	flushdb(config)

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	h := core.InfoHashFixture()
	origins := []*core.PeerInfo{core.OriginPeerInfoFixture()}

	require.NoError(s.UpdateOrigins(h, origins))

	result, err := s.GetOrigins(h)
	require.NoError(err)
	require.Len(result, 1)

	time.Sleep(2 * time.Second)

	result, err = s.GetOrigins(h)
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

func TestRedisStorageMetaInfoExpiration(t *testing.T) {
	require := require.New(t)

	config := redisConfigFixture()
	config.MetaInfoTTL = 2 * time.Second

	flushdb(config)

	s, err := NewRedisStorage(config, clock.New())
	require.NoError(err)

	mi := core.MetaInfoFixture()

	require.NoError(s.SetMetaInfo(mi))
	_, err = s.GetMetaInfo(mi.Name())
	require.NoError(err)

	time.Sleep(3 * time.Second)

	_, err = s.GetMetaInfo(mi.Name())
	require.Equal(ErrNotFound, err)
}
