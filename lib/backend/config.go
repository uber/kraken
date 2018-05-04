package backend

import (
	"code.uber.internal/infra/kraken/lib/backend/hdfsbackend"
	"code.uber.internal/infra/kraken/lib/backend/httpbackend"
	"code.uber.internal/infra/kraken/lib/backend/originbackend"
	"code.uber.internal/infra/kraken/lib/backend/s3backend"
	"code.uber.internal/infra/kraken/lib/backend/testfs"
	"code.uber.internal/infra/kraken/lib/backend/trackerbackend"
)

// NamespaceConfig defines a mapping of namespace regex to Config.
type NamespaceConfig map[string]Config

// Auth defines auth credentials for corresponding namespaces
// It has to be different due to langley secrets overlay structure
type Auth map[string]AuthConfig

// Config defines the union of configuration for all backends, where
// the Backend field serves as the key for which backend is activated.
type Config struct {
	Backend string                `yaml:"backend"`
	S3      s3backend.Config      `yaml:"s3"`
	HDFS    hdfsbackend.Config    `yaml:"hdfs"`
	Tracker trackerbackend.Config `yaml:"tracker"`
	TestFS  testfs.Config         `yaml:"testfs"`
	HTTP    httpbackend.Config    `yaml:"http"`
	Origin  originbackend.Config  `yaml:"origin"`
}

// AuthConfig defines the union of authentication credentials
// for all type of remote backends. s3 only supported currently
type AuthConfig struct {
	S3 s3backend.UserAuthConfig `yaml:"s3"`
}
