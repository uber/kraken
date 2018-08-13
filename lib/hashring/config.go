package hashring

// Config defines Ring configuration.
type Config struct {
	// MaxReplica is the max number of hosts each blob will be replicated across.
	// If MaxReplica is >= the number of hosts in the ring, every host will own
	// every blob.
	MaxReplica int `yaml:"max_replica"`
}

func (c *Config) applyDefaults() {
	if c.MaxReplica == 0 {
		c.MaxReplica = 3
	}
}
