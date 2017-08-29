package storage

import (
	"testing"

	"code.uber.internal/infra/kraken/config/tracker"
	"github.com/stretchr/testify/require"
)

func TestRedisStoresBackedBySameInstance(t *testing.T) {
	require := require.New(t)

	cfg := config.InitializeTest()
	cfg.Database.PeerStore = "redis"
	cfg.Database.TorrentStore = "redis"

	peerStore, err := GetPeerStore(cfg.Database)
	require.NoError(err)

	torrentStore, err := GetTorrentStore(cfg.Database)
	require.NoError(err)

	s1, ok := peerStore.(*RedisStorage)
	require.True(ok)

	s2, ok := torrentStore.(*RedisStorage)
	require.True(ok)

	require.Equal(s1, s2)
}

func TestMySQLStoresBackedBySameInstance(t *testing.T) {
	require := require.New(t)

	cfg := config.InitializeTest()
	cfg.Database.PeerStore = "mysql"
	cfg.Database.TorrentStore = "mysql"
	cfg.Database.ManifestStore = "mysql"

	peerStore, err := GetPeerStore(cfg.Database)
	require.NoError(err)

	torrentStore, err := GetTorrentStore(cfg.Database)
	require.NoError(err)

	manifestStore, err := GetManifestStore(cfg.Database)
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
