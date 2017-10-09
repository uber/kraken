package store

import "time"

// Config contains store directory configs
// TODO: merge them into one root dir
type Config struct {
	UploadDir     string              `yaml:"upload_dir"`
	DownloadDir   string              `yaml:"download_dir"`
	CacheDir      string              `yaml:"cache_dir"`
	TrashDir      string              `yaml:"trash_dir"`
	TrashDeletion TrashDeletionConfig `yaml:"trash_deletion"`
	LRUConfig     LRUConfig           `yaml:"lru"`
}

// TrashDeletionConfig contains configuration to delete trash dir
type TrashDeletionConfig struct {
	Enable   bool          `yaml:"enable"`
	Interval time.Duration `yaml:"interval"`
}

// LRUConfig contains configuration create a lru file store
type LRUConfig struct {
	Enable bool `yaml:"enable"`
	Size   int  `yaml:"size"`
}
