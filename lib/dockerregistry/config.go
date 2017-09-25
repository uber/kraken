package dockerregistry

import docker "github.com/docker/distribution/configuration"

// Config contains docker registry config, disable torrent flag, and tag deletion config
type Config struct {
	Docker         docker.Configuration `yaml:"docker"`
	DisableTorrent bool                 `yaml:"disable_torrent"`
	TagDir         string               `yaml:"tag_dir"`
	TagDeletion    TagDeletionConfig    `yaml:"tag_deletion"`
}

// TagDeletionConfig contains configuration to delete tags
type TagDeletionConfig struct {
	Enable bool `yaml:"enable"`
	// Interval for running tag deletion in seconds
	Interval int `yaml:"interval"`
	// Number of tags we keep for each repo
	RetentionCount int `yaml:"retention_count"`
	// Least number of seconds we keep tags for
	RetentionTime int `yaml:"retention_time"`
}
