package configuration

import (
	"os"
	"path"

	"net"

	xconfig "code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
	rc "github.com/docker/distribution/configuration"
)

const (
	defaultConfigDir = "config"
	configDirKey     = "UBER_CONFIG_DIR"
)

// Config contains application configuration
type Config struct {
	Environment      string           `yaml:"environment"`
	DownloadDir      string           `yaml:"download_dir"`
	CacheDir         string           `yaml:"cache_dir"`
	CacheSize        int              `yaml:"cache_size"`
	CacheMapSize     int              `yaml:"cache_map_size"`
	RedisURL         string           `yaml:"redis_url"`
	PieceLength      int              `yaml:"piece_length"`
	Announce         string           `yaml:"announce"`
	AnnounceInterval int              `yaml:"announce_interval"`
	ExpireSec        int              `yaml:"expire_sec"`
	ClientAddr       string           `yaml:"client"`
	PushTempDir      string           `yaml:"push_temp_dir"`
	RegistryPort     string           `yaml:"registry_port"`
	Notifications    rc.Notifications `yaml:"notifications,omitempty"`
}

// NewConfig returns a configuration frocvQa234	287m a YAML file
func NewConfig(configPath string) *Config {
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

// GetClientPort returns listen port
func (c *Config) GetClientPort() (string, error) {
	_, port, err := net.SplitHostPort(c.ClientAddr)
	if err != nil {
		return "", err
	}

	return port, nil
}
