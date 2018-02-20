package hdfsbackend

import (
	"errors"

	"github.com/c2h5oh/datasize"

	"code.uber.internal/infra/kraken/utils/memsize"
)

// Config defines configuration for all HDFS clients.
type Config struct {
	NameNodes []string `yaml:"namenodes"`
	BuffSize  int64    `yaml:"buff_size"` // Default transfer block size.
	UserName  string   `yaml:"username"`  // Auth username.

	// BufferGuard protects upload from draining the src reader into an oversized
	// buffer when io.Seeker is not implemented.
	BufferGuard datasize.ByteSize `yaml:"buffer_guard"`
}

func (c Config) applyDefaults() (Config, error) {
	if len(c.NameNodes) == 0 {
		return Config{}, errors.New("namenodes required")
	}
	if c.BuffSize == 0 {
		c.BuffSize = int64(64 * memsize.MB)
	}
	if c.BufferGuard == 0 {
		c.BufferGuard = 10 * datasize.MB
	}
	return c, nil
}
