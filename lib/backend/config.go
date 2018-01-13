package backend

import (
	"code.uber.internal/infra/kraken/lib/backend/s3backend"
	"code.uber.internal/infra/kraken/lib/backend/testfs"
)

// NamespaceConfig defines a mapping of namespace identifier to Config.
type NamespaceConfig map[string]Config

// Config defines the union of configuration for all backends, where
// the Backend field serves as the key for which backend is activated.
type Config struct {
	Backend string           `yaml:"backend"`
	S3      s3backend.Config `yaml:"s3"`
	TestFS  testfs.Config    `yaml:"testfs"`
}
