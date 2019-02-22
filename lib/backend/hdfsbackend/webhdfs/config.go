package webhdfs

import "github.com/c2h5oh/datasize"

// Config defines Client configuration.
type Config struct {
	// BufferSize is the transfer block size.
	BufferSize datasize.ByteSize `yaml:"buffer_size"`

	// BufferGuard protects upload from draining the src reader into an oversized
	// buffer when io.Seeker is not implemented.
	BufferGuard datasize.ByteSize `yaml:"buffer_guard"`
}

func (c *Config) applyDefaults() {
	if c.BufferSize == 0 {
		c.BufferSize = 64 * datasize.MB
	}
	if c.BufferGuard == 0 {
		c.BufferGuard = 10 * datasize.MB
	}
}
