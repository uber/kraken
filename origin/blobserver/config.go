package blobserver

import (
	"time"

	"code.uber.internal/infra/kraken/utils/listener"
)

// Config defines the configuration used by Origin cluster for hashing blob digests.
type Config struct {
	Listener                  listener.Config `yaml:"listener"`
	DuplicateWriteBackStagger time.Duration   `yaml:"duplicate_write_back_stagger"`
}

func (c Config) applyDefaults() Config {
	if c.DuplicateWriteBackStagger == 0 {
		c.DuplicateWriteBackStagger = 30 * time.Minute
	}
	return c
}
