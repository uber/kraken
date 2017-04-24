package configuration

import (
	"fmt"
	"os"
	"path"

	"golang.org/x/time/rate"

	xconfig "code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken-torrent"
	"code.uber.internal/infra/kraken-torrent/storage"
	rc "github.com/docker/distribution/configuration"
)

const (
	defaultConfigDir = "config"
	configDirKey     = "UBER_CONFIG_DIR"
)

// Agent contains configuration of bittorrent agent
type Agent struct {
	PieceLength        int  `yaml:"piece_length"`
	Frontend           int  `yaml:"frontend"`
	Backend            int  `yaml:"backend"`
	Seed               bool `yaml:"seed"`
	Debug              bool `yaml:"debug"`
	NoDHT              bool `yaml:"noDHT"`
	NoUpload           bool `yaml:"noUpload"`
	DisableTCP         bool `yaml:"disableTCP"`
	DisableUTP         bool `yaml:"disableUTP"`
	DisableEncryption  bool `yaml:"disableEncryption"`
	ForceEncryption    bool `yaml:"forceEncryption"`
	PreferNoEncryption bool `yaml:"preferNoEncryption"`
	Download           struct {
		Rate  int `yaml:"rate"`
		Limit int `yaml:"limit"`
	} `yaml:"download"`
	Upload struct {
		Rate  int `yaml:"rate"`
		Limit int `yaml:"limit"`
	} `yaml:"upload"`
}

// Config contains application configuration
type Config struct {
	Environment string `yaml:"environment"`
	// This is used for docker registry only running locally
	DisableTorrent bool             `yaml:"disable_torrent"`
	UploadDir      string           `yaml:"upload_dir"`
	DownloadDir    string           `yaml:"download_dir"`
	CacheDir       string           `yaml:"cache_dir"`
	TrashDir       string           `yaml:"trash_dir"`
	TagDir         string           `yaml:"tag_dir"`
	TrackerURL     string           `yaml:"tracker_url"`
	Registry       rc.Configuration `yaml:"registry"`
	Agent          Agent            `yaml:"agent"`
	TagDeletion    struct {
		Enable bool `yaml:"enable"`
		// Interval for running tag deletion
		Interval int `yaml:"interval"`
		// Number of tags we keep for each repo
		Retention int `yaml:"retention"`
	} `yaml:"tag_deletion"`
	TrashGC struct {
		Enable bool `yaml:"enable"`
		// Interval for running tag deletion
		Interval int `yaml:"interval"`
	} `yaml:"trash_gc"`
}

// NewConfig creates a configuration based on environment var
func NewConfig() *Config {
	var c Config
	if err := xconfig.Load(&c); err != nil {
		log.Fatal(err)
	}
	log.Info("Configuration loaded.")
	return &c
}

// NewConfigWithPath creates a configuration given a YAML file
func NewConfigWithPath(configPath string) *Config {
	var c Config
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatalf("Cannot find config file: %s", configPath)
	}
	log.Info("Loading configuration from '", configPath, "'")

	if err := xconfig.LoadFile(configPath, &c); err != nil {
		log.Fatal(err)
	}

	log.Info("Configuration loaded.")
	return &c
}

// GetConfigFilePath returns absolute path of test.yaml
func GetConfigFilePath(filename string) string {
	// Generate test config path for go-build
	var realConfigDir string
	// Allow overriding the directory config is loaded from, useful for tests
	// inside subdirectories when the config/ dir is in the top-level of a project.
	if configRoot := os.Getenv(configDirKey); configRoot != "" {
		realConfigDir = configRoot
	} else {
		realConfigDir = defaultConfigDir
	}
	configFile := path.Join(realConfigDir, filename)
	return configFile
}

// CreateAgentConfig returns torrent agent's configuration
func (c *Config) CreateAgentConfig(storage storage.ClientImpl) *torrent.Config {
	var dl *rate.Limiter
	var upl *rate.Limiter
	acfg := c.Agent

	if acfg.Download.Limit > 0 {
		dl = rate.NewLimiter(rate.Limit(acfg.Download.Limit), acfg.Download.Rate)
	} else {
		dl = rate.NewLimiter(rate.Inf, 1)
	}

	if acfg.Upload.Limit > 0 {
		upl = rate.NewLimiter(rate.Limit(acfg.Upload.Limit), acfg.Upload.Rate)
	} else {
		upl = rate.NewLimiter(rate.Inf, 1)
	}

	return &torrent.Config{
		DefaultStorage:      storage,
		Seed:                acfg.Seed,
		ListenAddr:          fmt.Sprintf("0.0.0.0:%d", acfg.Backend),
		NoUpload:            acfg.NoUpload,
		DisableTCP:          acfg.DisableTCP,
		NoDHT:               acfg.NoDHT,
		Debug:               acfg.Debug,
		DisableUTP:          acfg.DisableUTP,
		DisableEncryption:   acfg.DisableEncryption,
		ForceEncryption:     acfg.ForceEncryption,
		PreferNoEncryption:  acfg.PreferNoEncryption,
		DownloadRateLimiter: dl,
		UploadRateLimiter:   upl,
	}
}
