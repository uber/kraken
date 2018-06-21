package store

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

// CADownloadStoreConfig defines CADownloadStore configuration.
type CADownloadStoreConfig struct {
	DownloadDir     string        `yaml:"download_dir"`
	CacheDir        string        `yaml:"cache_dir"`
	DownloadCleanup CleanupConfig `yaml:"download_cleanup"`
	CacheCleanup    CleanupConfig `yaml:"cache_cleanup"`
}
