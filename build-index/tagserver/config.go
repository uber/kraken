package tagserver

import (
	"time"

	"code.uber.internal/infra/kraken/utils/dedup"
)

// Config defines Server configuration.
type Config struct {
	Cache                     dedup.CacheConfig `yaml:"cache"`
	DuplicateReplicateStagger time.Duration     `yaml:"duplicate_replicate_stagger"`
}

func (c Config) applyDefaults() Config {
	if c.DuplicateReplicateStagger == 0 {
		c.DuplicateReplicateStagger = 20 * time.Minute
	}
	return c
}
