package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisStoresBackedBySameInstance(t *testing.T) {
	require := require.New(t)

	cfg := configFixture()
	cfg.PeerStore = "redis"
	cfg.MetaInfoStore = "redis"

	storeProvider := NewStoreProvider(cfg, nemoConfigFixture())

	peerStore1, err := storeProvider.GetPeerStore()
	require.NoError(err)

	peerStore2, err := storeProvider.GetPeerStore()
	require.NoError(err)

	s1, ok := peerStore1.(*RedisStorage)
	require.True(ok)

	s2, ok := peerStore2.(*RedisStorage)
	require.True(ok)

	require.Equal(s1, s2)
}

func TestMySQLStoresBackedBySameInstance(t *testing.T) {
	require := require.New(t)

	cfg := configFixture()
	cfg.PeerStore = "mysql"
	cfg.MetaInfoStore = "mysql"
	cfg.ManifestStore = "mysql"

	storeProvider := NewStoreProvider(cfg, nemoConfigFixture())

	torrentStore, err := storeProvider.GetMetaInfoStore()
	require.NoError(err)

	manifestStore, err := storeProvider.GetManifestStore()
	require.NoError(err)

	s1, ok := torrentStore.(*MySQLStorage)
	require.True(ok)

	s2, ok := manifestStore.(*MySQLStorage)
	require.True(ok)

	require.Equal(s1, s2)
}
