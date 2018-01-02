package backend

import "code.uber.internal/infra/kraken/lib/backend/s3"

// NamespaceConfig defines a mapping of namespace identifier to Config.
type NamespaceConfig map[string]Config

// Config defines the union of configuration for all backends, where
// the Backend field serves as the key for which backend is activated.
type Config struct {
	Backend string    `yaml:"backend"`
	S3      s3.Config `yaml:"s3"`
}
