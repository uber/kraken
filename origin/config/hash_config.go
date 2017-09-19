package config

import (
	"os"

	xconfig "code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
)

// HashConfig defines the configuration used by Origin cluster for hashing blob digests.
type HashConfig struct {
	Logging    log.Configuration
	Verbose    bool
	NumReplica int                       `yaml:"num_replica"`
	HashNodes  map[string]HashNodeConfig `yaml:"hash_nodes"`

	Label           string            `yaml:"-"`
	Hostname        string            `yaml:"-"`
	LabelToHostname map[string]string `yaml:"-"`
}

// HashNodeConfig defines the config for a single origin node
type HashNodeConfig struct {
	Label  string `yaml:"label"`
	Weight int    `yaml:"weight"`
}

// StringSet models a set of strings
type StringSet map[string]struct{}

// UnmarshalYAML unmarshals YAML into a StringSet
func (s *StringSet) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var stringList []string
	if err := unmarshal(&stringList); err != nil {
		return err
	}

	if *s == nil {
		*s = make(StringSet)
	}
	for _, str := range stringList {
		(*s)[str] = struct{}{}
	}
	return nil
}

// Initialize initializes the global config
func Initialize() HashConfig {
	var cfg HashConfig
	if err := xconfig.Load(&cfg); err != nil {
		log.Fatalf("Error initializing configuration: %s", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Error initializing hostname in configuration: %s", err)
	}
	cfg.Hostname = hostname
	currNode, ok := cfg.HashNodes[hostname]
	if !ok {
		log.Fatalf("Error initializing label in configuration: %s", err)
	}
	cfg.Label = currNode.Label

	cfg.LabelToHostname = make(map[string]string, len(cfg.HashNodes))
	for hostname, node := range cfg.HashNodes {
		cfg.LabelToHostname[node.Label] = hostname
	}

	return cfg
}
