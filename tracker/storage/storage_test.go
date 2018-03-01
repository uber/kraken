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

	storeProvider := NewStoreProvider(cfg)

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
