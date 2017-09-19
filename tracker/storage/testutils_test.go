package storage

import (
	"time"

	"code.uber.internal/go-common.git/x/mysql"
	"code.uber.internal/infra/kraken/config/tracker"
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

func redisConfigFixture() config.RedisConfig {
	return config.RedisConfig{
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

func mysqlConfigFixture() config.MySQLConfig {
	return config.MySQLConfig{
		MigrationsDir: "db/migrations",
	}
}

func databaseConfigFixture() config.DatabaseConfig {
	return config.DatabaseConfig{
		PeerStore:     "redis",
		TorrentStore:  "mysql",
		ManifestStore: "mysql",
		MySQL:         mysqlConfigFixture(),
		Redis:         redisConfigFixture(),
	}
}
