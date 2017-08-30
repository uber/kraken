package configuration

import (
	"os"
	"path"

	xconfig "code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/client/dockerregistry"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrent"
)

const (
	defaultConfigDir = "config"
	configDirKey     = "UBER_CONFIG_DIR"
)

// Config contains application configuration
type Config struct {
	Environment string                 `yaml:"environment"`
	Metrics     map[string]interface{} `yaml:"metrics"`

	Store    store.Config          `yaml:"store"`
	Registry dockerregistry.Config `yaml:"registry"`
	Torrent  torrent.Config        `yaml:"torrent"`
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
