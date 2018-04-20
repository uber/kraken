package hdfsbackend

import (
	"github.com/c2h5oh/datasize"

	"code.uber.internal/infra/kraken/utils/memsize"
)

// Config defines configuration for all HDFS clients.
type Config struct {
	NameNodes []string `yaml:"namenodes"`
	BuffSize  int64    `yaml:"buff_size"` // Default transfer block size.
	UserName  string   `yaml:"username"`  // Auth username.

	RootDirectory string `yaml:"root_directory"` // RootDirectory for WebHDFS docker registry path, default

	// BufferGuard protects upload from draining the src reader into an oversized
	// buffer when io.Seeker is not implemented.
	BufferGuard datasize.ByteSize `yaml:"buffer_guard"`

	// NamePath identifies which namepath.Pather to use.
	NamePath string `yaml:"name_path"`
}

func (c Config) applyDefaults() Config {
	if c.BuffSize == 0 {
		c.BuffSize = int64(64 * memsize.MB)
	}
	if c.BufferGuard == 0 {
		c.BufferGuard = 10 * datasize.MB
	}
	if c.RootDirectory == "" {
		c.RootDirectory = "webhdfs/v1/infra/dockerRegistry/"
	}
	return c
}
