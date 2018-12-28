package peerstore

import (
	"time"
)

// Config defines Store configuration.
type Config struct {
	Redis RedisConfig `yaml:"redis"`
}

// RedisConfig defines RedisStore configuration.
// TODO(evelynl94): rename
type RedisConfig struct {
	Addr              string        `yaml:"addr"`
	DialTimeout       time.Duration `yaml:"dial_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	PeerSetWindowSize time.Duration `yaml:"peer_set_window_size"`
	MaxPeerSetWindows int           `yaml:"max_peer_set_windows"`
	MaxIdleConns      int           `yaml:"max_idle_conns"`
	MaxActiveConns    int           `yaml:"max_active_conns"`
	IdleConnTimeout   time.Duration `yaml:"idle_conn_timeout"`
}

func (c *RedisConfig) applyDefaults() {
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 30 * time.Second
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 30 * time.Second
	}
	if c.PeerSetWindowSize == 0 {
		c.PeerSetWindowSize = time.Hour
	}
	if c.MaxPeerSetWindows == 0 {
		c.MaxPeerSetWindows = 5
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 10
	}
	if c.MaxActiveConns == 0 {
		c.MaxActiveConns = 500
	}
	if c.IdleConnTimeout == 0 {
		c.IdleConnTimeout = 60 * time.Second
	}
}
