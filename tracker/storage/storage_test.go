package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisStoresBackedBySameInstance(t *testing.T) {
	require := require.New(t)

	cfg := configFixture()
	cfg.PeerStore = "redis"
	cfg.TorrentStore = "redis"

	storeProvider := NewStoreProvider(cfg, nemoConfigFixture())

	peerStore, err := storeProvider.GetPeerStore()
	require.NoError(err)

	torrentStore, err := storeProvider.GetTorrentStore()
	require.NoError(err)

	s1, ok := peerStore.(*RedisStorage)
	require.True(ok)

	s2, ok := torrentStore.(*RedisStorage)
	require.True(ok)

	require.Equal(s1, s2)
}

func TestMySQLStoresBackedBySameInstance(t *testing.T) {
	require := require.New(t)

	cfg := configFixture()
	cfg.PeerStore = "mysql"
	cfg.TorrentStore = "mysql"
	cfg.ManifestStore = "mysql"

	storeProvider := NewStoreProvider(cfg, nemoConfigFixture())

	peerStore, err := storeProvider.GetPeerStore()
	require.NoError(err)

	torrentStore, err := storeProvider.GetTorrentStore()
	require.NoError(err)

	manifestStore, err := storeProvider.GetManifestStore()
	require.NoError(err)

	s1, ok := peerStore.(*MySQLStorage)
	require.True(ok)

	s2, ok := torrentStore.(*MySQLStorage)
	require.True(ok)

	s3, ok := manifestStore.(*MySQLStorage)
	require.True(ok)

	require.Equal(s1, s2)
	require.Equal(s2, s3)
}
