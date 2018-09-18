package hdfsbackend

import "code.uber.internal/infra/kraken/lib/backend/hdfsbackend/webhdfs"

// Config defines configuration for all HDFS clients.
type Config struct {
	NameNodes     []string `yaml:"namenodes"`
	UserName      string   `yaml:"username"`
	RootDirectory string   `yaml:"root_directory"`

	// ListConcurrency is the number of threads used for listing.
	ListConcurrency int `yaml:"list_concurrency"`

	// NamePath identifies which namepath.Pather to use.
	NamePath string `yaml:"name_path"`

	WebHDFS webhdfs.Config `yaml:"webhdfs"`
}

func (c *Config) applyDefaults() {
	if c.RootDirectory == "" {
		c.RootDirectory = "webhdfs/v1/infra/dockerRegistry/"
	}
	if c.ListConcurrency == 0 {
		c.ListConcurrency = 4
	}
}
