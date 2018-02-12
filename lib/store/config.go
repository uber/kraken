package store

import "time"

// Config contains store directory configs
// TODO: merge them into one root dir
type Config struct {
	UploadDir       string                `yaml:"upload_dir"`
	DownloadDir     string                `yaml:"download_dir"`
	CacheDir        string                `yaml:"cache_dir"`
	TrashDir        string                `yaml:"trash_dir"`
	TrashDeletion   TrashDeletionConfig   `yaml:"trash_deletion"`
	DownloadCleanup DownloadCleanupConfig `yaml:"download_cleanup"`
	LRUConfig       LRUConfig             `yaml:"lru"`
}

func (c Config) applyDefaults() Config {
	c.DownloadCleanup = c.DownloadCleanup.applyDefaults()
	return c
}

// TrashDeletionConfig contains configuration to delete trash dir
type TrashDeletionConfig struct {
	Enable   bool          `yaml:"enable"`
	Interval time.Duration `yaml:"interval"`
}

// DownloadCleanupConfig defines configuration for cleaning up files in the
// download directory.
type DownloadCleanupConfig struct {
	Enabled bool `yaml:"enabled"`

	// Interval defines how often cleanup will run.
	Interval time.Duration `yaml:"interval"`

	// TTI defines the duration a download file can exist without being written
	// to before being declared idle and deleted.
	TTI time.Duration `yaml:"tti"`
}

func (c DownloadCleanupConfig) applyDefaults() DownloadCleanupConfig {
	if c.Interval == 0 {
		c.Interval = 30 * time.Minute
	}
	if c.TTI == 0 {
		c.TTI = 24 * time.Hour
	}
	return c
}

// LRUConfig contains configuration create a lru file store
type LRUConfig struct {
	Enable   bool          `yaml:"enable"`
	Size     int           `yaml:"size"`
	TTL      time.Duration `yaml:"ttl"`
	Interval time.Duration `yaml:"interval"`
}
