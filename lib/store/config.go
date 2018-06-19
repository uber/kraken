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

// Volume - if provided, volumes are used to store the actual files.
// Symlinks will be created under state directories.
// This configuration is needed on hosts with multiple disks.
type Volume struct {
	Location string
	Weight   int
}

// CAStoreConfig defines CAStore configuration.
type CAStoreConfig struct {
	UploadDir     string        `yaml:"upload_dir"`
	CacheDir      string        `yaml:"cache_dir"`
	Volumes       []Volume      `yaml:"volumes"`
	Capacity      int           `yaml:"capacity"`
	UploadCleanup CleanupConfig `yaml:"upload_cleanup"`
	CacheCleanup  CleanupConfig `yaml:"cache_cleanup"`
}

func (c CAStoreConfig) applyDefaults() CAStoreConfig {
	if c.Capacity == 0 {
		c.Capacity = 1 << 20 // 1 million
	}
	return c
}

// SimpleStoreConfig defines SimpleStore configuration.
type SimpleStoreConfig struct {
	UploadDir     string        `yaml:"upload_dir"`
	CacheDir      string        `yaml:"cache_dir"`
	UploadCleanup CleanupConfig `yaml:"upload_cleanup"`
	CacheCleanup  CleanupConfig `yaml:"cache_cleanup"`
}

// TorrentStoreConfig defines TorrentStore configuration.
type TorrentStoreConfig struct {
	DownloadDir     string        `yaml:"download_dir"`
	CacheDir        string        `yaml:"cache_dir"`
	DownloadCleanup CleanupConfig `yaml:"download_cleanup"`
	CacheCleanup    CleanupConfig `yaml:"cache_cleanup"`
}
