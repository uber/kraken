package hdfsbackend

import (
	"errors"

	"code.uber.internal/infra/kraken/utils/memsize"
)

// Config defines configuration for all HDFS clients.
type Config struct {
	NameNodes []string `yaml:"namenodes"`
	BuffSize  int64    `yaml:"buff_size"` // Default transfer block size.
	UserName  string   `yaml:"username"`  // Auth username.
}

func (c Config) applyDefaults() (Config, error) {
	if len(c.NameNodes) == 0 {
		return Config{}, errors.New("namenodes required")
	}
	if c.BuffSize == 0 {
		c.BuffSize = int64(64 * memsize.MB)
	}
	return c, nil
}
