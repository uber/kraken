package storage

import (
	"time"
)

func redisConfigFixture() RedisConfig {
	return RedisConfig{
		Addr:              "localhost:6380",
		PeerSetWindowSize: 30 * time.Second,
		MaxPeerSetWindows: 4,
		OriginsTTL:        5 * time.Minute,
	}
}

func configFixture() Config {
	return Config{
		PeerStore:     "redis",
		MetaInfoStore: "redis",
		Redis:         redisConfigFixture(),
	}
}
