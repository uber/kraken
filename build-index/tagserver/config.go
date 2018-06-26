package tagserver

import (
	"time"
)

// Config defines Server configuration.
type Config struct {
	DuplicateReplicateStagger time.Duration `yaml:"duplicate_replicate_stagger"`
	DuplicatePutStagger       time.Duration `yaml:"duplicate_put_stagger"`
}

func (c Config) applyDefaults() Config {
	if c.DuplicateReplicateStagger == 0 {
		c.DuplicateReplicateStagger = 20 * time.Minute
	}
	if c.DuplicatePutStagger == 0 {
		c.DuplicatePutStagger = 20 * time.Minute
	}
	return c
}
