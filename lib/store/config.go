package store

import "time"

// Config contains store directory configs
// TODO: merge them into one root dir
type Config struct {
	UploadDir       string        `yaml:"upload_dir"`
	DownloadDir     string        `yaml:"download_dir"`
	CacheDir        string        `yaml:"cache_dir"`
	LRUConfig       LRUConfig     `yaml:"lru"`
	DownloadCleanup CleanupConfig `yaml:"download_cleanup"`
	CacheCleanup    CleanupConfig `yaml:"cache_cleanup"`
}

func (c Config) applyDefaults() Config {
	c.DownloadCleanup = c.DownloadCleanup.applyDefaults()
	return c
}

// LRUConfig contains configuration create a lru file store
type LRUConfig struct {
	Enable   bool          `yaml:"enable"`
	Size     int           `yaml:"size"`
	TTL      time.Duration `yaml:"ttl"`
	Interval time.Duration `yaml:"interval"`
}

// OriginConfig contains directory configs for origin file store.
type OriginConfig struct {
	UploadDir     string        `yaml:"upload_dir"`
	CacheDir      string        `yaml:"cache_dir"`
	Volumes       []Volume      `yaml:"volumes"`
	Capacity      int           `yaml:"capacity"` // TODO: change to disk space instead of object count
	UploadCleanup CleanupConfig `yaml:"upload_cleanup"`
	CacheCleanup  CleanupConfig `yaml:"cache_cleanup"`
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
