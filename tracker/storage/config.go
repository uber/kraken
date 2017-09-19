package storage

import "time"

// Config defines storage configuration.
type Config struct {
	PeerStore     string      `yaml:"peer_store"`
	TorrentStore  string      `yaml:"torrent_store"`
	ManifestStore string      `yaml:"manifest_store"`
	Redis         RedisConfig `yaml:"redis"`
	MySQL         MySQLConfig `yaml:"mysql"`
}

// MySQLConfig defines configuration for MySQL storage.
type MySQLConfig struct {
	MigrationsDir string `yaml:"migration_dir"`
}

// RedisConfig defines configuration for Redis storage.
type RedisConfig struct {
	Addr                  string        `yaml:"addr"`
	DialTimeout           time.Duration `yaml:"dial_timeout"`
	ReadTimeout           time.Duration `yaml:"read_timeout"`
	WriteTimeout          time.Duration `yaml:"write_timeout"`
	PeerSetWindowSizeSecs int           `yaml:"peer_set_window_size_secs"`
	MaxPeerSetWindows     int           `yaml:"max_peer_set_windows"`
	TorrentTTLSecs        int           `yaml:"torrent_ttl_secs"`
	MaxIdleConns          int           `yaml:"max_idle_conns"`
	MaxActiveConns        int           `yaml:"max_active_conns"`
	IdleConnTimeoutSecs   int           `yaml:"idle_conn_timeout_secs"`
}
