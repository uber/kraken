package connstate

import "time"

// Config defines State configuration.
type Config struct {

	// MaxOpenConnectionsPerTorrent is the maximum number of connections which a
	// Scheduler will maintain at once for each torrent.
	MaxOpenConnectionsPerTorrent int `yaml:"max_open_conn"`

	// MaxMutualConnections is the maximum number of mutual connections a peer
	// can have and still connect with us.
	MaxMutualConnections int `yaml:"max_mutual_conn"`

	// DisableBlacklist disables the blacklisting of peers. Should only be used
	// for testing purposes.
	DisableBlacklist bool `yaml:"disable_blacklist"`

	// BlacklistDuration is the duration a connection will remain blacklisted.
	BlacklistDuration time.Duration `yaml:"blacklist_duration"`
}

func (c Config) applyDefaults() Config {
	if c.MaxOpenConnectionsPerTorrent == 0 {
		c.MaxOpenConnectionsPerTorrent = 10
	}
	if c.MaxMutualConnections == 0 {
		c.MaxMutualConnections = 10
	}
	if c.BlacklistDuration == 0 {
		c.BlacklistDuration = 30 * time.Second
	}
	return c
}
