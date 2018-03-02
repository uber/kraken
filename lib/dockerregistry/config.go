package dockerregistry

import (
	"time"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"
	docker "github.com/docker/distribution/configuration"
	"github.com/uber-go/tally"
)

// Config contains docker registry config, disable torrent flag, and tag deletion config
type Config struct {
	Docker         docker.Configuration `yaml:"docker"`
	DisableTorrent bool                 `yaml:"disable_torrent"`
	TagDir         string               `yaml:"tag_dir"`
	TagDeletion    TagDeletionConfig    `yaml:"tag_deletion"`

	Namespaces backend.NamespaceConfig `yaml:"namespaces"`

	TagNamespace  string `yaml:"tag_namespace"`
	BlobNamespace string `yaml:"blob_namespace"`
}

func (c Config) applyDefaults() Config {
	c.TagDeletion = c.TagDeletion.applyDefaults()
	return c
}

// TagDeletionConfig contains configuration to delete tags
type TagDeletionConfig struct {
	Enable bool `yaml:"enable"`

	// Interval for running tag deletion.
	Interval time.Duration `yaml:"interval"`

	// Number of tags we keep for each repo
	RetentionCount int `yaml:"retention_count"`

	// Duration tags are kept for.
	RetentionTime time.Duration `yaml:"retention_time"`
}

func (c TagDeletionConfig) applyDefaults() TagDeletionConfig {
	if c.Interval == 0 {
		c.Interval = 30 * time.Minute
	}
	if c.RetentionTime == 0 {
		c.RetentionTime = 24 * time.Hour
	}
	return c
}

// CreateDockerConfig returns docker specified configuration
func (c Config) CreateDockerConfig(name string, imageTransferer transfer.ImageTransferer, fileStore store.FileStore, stats tally.Scope) *docker.Configuration {
	c.Docker.Storage = docker.Storage{
		name: docker.Parameters{
			"config":     c,
			"transferer": imageTransferer,
			"store":      fileStore,
			"metrics":    stats,
		},
		// Redirect is enabled by default in docker registry.
		// We implement redirect on proxy level so we do not need this in storage driver for now.
		"redirect": docker.Parameters{
			"disable": true,
		},
	}
	return &c.Docker
}
