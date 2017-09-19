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
		Addr:                  "localhost:6380",
		DialTimeout:           5 * time.Second,
		ReadTimeout:           30 * time.Second,
		WriteTimeout:          30 * time.Second,
		PeerSetWindowSizeSecs: 30,
		MaxPeerSetWindows:     4,
		TorrentTTLSecs:        60,
		MaxIdleConns:          5,
		MaxActiveConns:        20,
		IdleConnTimeoutSecs:   10,
	}
}

func mysqlConfigFixture() MySQLConfig {
	return MySQLConfig{
		MigrationsDir: "db/migrations",
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
