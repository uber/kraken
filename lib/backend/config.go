package backend

import (
	"code.uber.internal/infra/kraken/lib/backend/hdfsbackend"
	"code.uber.internal/infra/kraken/lib/backend/httpbackend"
	"code.uber.internal/infra/kraken/lib/backend/s3backend"
	"code.uber.internal/infra/kraken/lib/backend/terrablobbackend"
	"code.uber.internal/infra/kraken/lib/backend/testfs"
	"code.uber.internal/infra/kraken/utils/bandwidth"
	"code.uber.internal/infra/kraken/utils/memsize"
)

// Config defines the union of configuration for all backends, where
// the Backend field serves as the key for which backend is activated.
type Config struct {
	Namespace string                  `yaml:"namespace"`
	Backend   string                  `yaml:"backend"`
	S3        s3backend.Config        `yaml:"s3"`
	HDFS      hdfsbackend.Config      `yaml:"hdfs"`
	TestFS    testfs.Config           `yaml:"testfs"`
	HTTP      httpbackend.Config      `yaml:"http"`
	TerraBlob terrablobbackend.Config `yaml:"terrablob"`

	// If enabled, throttles upload / download bandwidth.
	Bandwidth bandwidth.Config `yaml:"bandwidth"`
}

func (c Config) applyDefaults() Config {
	if c.Backend == _s3 {
		if c.Bandwidth.IngressBitsPerSec == 0 {
			c.Bandwidth.IngressBitsPerSec = 10 * memsize.Gbit
		}
		if c.Bandwidth.EgressBitsPerSec == 0 {
			c.Bandwidth.EgressBitsPerSec = memsize.Gbit
		}
	}
	return c
}

// Auth defines auth credentials for corresponding namespaces
// It has to be different due to langley secrets overlay structure
type Auth map[string]AuthConfig

// AuthConfig defines the union of authentication credentials
// for all type of remote backends. s3 only supported currently
type AuthConfig struct {
	S3 s3backend.UserAuthConfig `yaml:"s3"`
}
