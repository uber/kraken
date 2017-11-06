package storage

import (
	"time"

	"code.uber.internal/go-common.git/x/mysql"
)

func nemoConfigFixture() mysql.Configuration {
	return mysql.Configuration{
		Clusters: map[string]mysql.Cluster{
			"local": {
				Databases: map[string]mysql.Database{
					"kraken": {
						User:     "uber",
						Password: "uber",
						Host:     "localhost",
						Port: mysql.Ports{
							Master: 3307,
						},
					},
				},
			},
		},
		DefaultCluster:  "local",
		DefaultDatabase: "kraken",
	}
}

func redisConfigFixture() RedisConfig {
	return RedisConfig{
		Addr:              "localhost:6380",
		PeerSetWindowSize: 30 * time.Second,
		MaxPeerSetWindows: 4,
		TorrentTTL:        time.Minute,
		OriginsTTL:        5 * time.Minute,
	}
}

func mysqlConfigFixture() MySQLConfig {
	return MySQLConfig{
		// Assumes the working directory is tracker/storage.
		MigrationsDir: "../../db/migrations",
	}
}

func configFixture() Config {
	return Config{
		PeerStore:     "redis",
		TorrentStore:  "mysql",
		ManifestStore: "mysql",
		MySQL:         mysqlConfigFixture(),
		Redis:         redisConfigFixture(),
	}
}
