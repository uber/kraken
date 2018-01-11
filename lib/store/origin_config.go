package store

import "time"

// OriginConfig contains directory configs for origin file store.
type OriginConfig struct {
	UploadDir string   `yaml:"upload_dir"`
	CacheDir  string   `yaml:"cache_dir"`
	Volumes   []Volume `yaml:"volumes"`

	Capacity        int           `yaml:"capacity"` // TODO: change to disk space instead of object count
	TTI             time.Duration `yaml:"tti"`      // TimeToIdle
	CleanupInterval time.Duration `yaml:"cleanup_interval"`
}

// Volume - if provided, volumes are used to store the actual files.
// Symlinks will be created under state directories.
// This configuration is needed on hosts with multiple disks.
type Volume struct {
	Location string
	Weight   int
}

func (c OriginConfig) applyDefaults() OriginConfig {
	if c.Capacity == 0 {
		c.Capacity = 1 << 20 // 1 million
	}
	return c
}
