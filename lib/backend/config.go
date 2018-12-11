package backend

import (
	"github.com/uber/kraken/utils/bandwidth"
	"github.com/uber/kraken/utils/memsize"
)

// Config defines the union of configuration for all backends. Only one is
// allowed for each config file.
type Config struct {
	Namespace string                 `yaml:"namespace"`
	Backend   map[string]interface{} `yaml:"backend"`

	// If enabled, throttles upload / download bandwidth.
	Bandwidth bandwidth.Config `yaml:"bandwidth"`
}

func (c Config) applyDefaults() Config {
	for k := range c.Backend {
		// TODO: don't hard code backend client name
		if k == "s3" {
			if c.Bandwidth.IngressBitsPerSec == 0 {
				c.Bandwidth.IngressBitsPerSec = 10 * 8 * memsize.Gbit
			}
			if c.Bandwidth.EgressBitsPerSec == 0 {
				c.Bandwidth.EgressBitsPerSec = 8 * memsize.Gbit
			}
		}
	}
	return c
}

// Auth defines auth credentials for corresponding namespaces.
// It has to be different due to langley secrets overlay structure.
type Auth map[string]AuthConfig

// AuthConfig defines the union of authentication credentials for all type of
// remote backends.
type AuthConfig map[string]interface{}
