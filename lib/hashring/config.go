package hashring

import "time"

// Config defines Ring configuration.
type Config struct {
	// MaxReplica is the max number of hosts each blob will be replicated across.
	// If MaxReplica is >= the number of hosts in the ring, every host will own
	// every blob.
	MaxReplica int `yaml:"max_replica"`

	// RefreshInterval is the interval at which membership / health information
	// is refreshed during monitoring.
	RefreshInterval time.Duration `yaml:"refresh_interval"`
}

func (c *Config) applyDefaults() {
	if c.MaxReplica == 0 {
		c.MaxReplica = 3
	}
	if c.RefreshInterval == 0 {
		c.RefreshInterval = 10 * time.Second
	}
}
