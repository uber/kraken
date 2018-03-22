package originbackend

import "code.uber.internal/infra/kraken/lib/serverset"

// Config defines Client configuration.
type Config struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`

	// If set, DisableUploadThrough will only upload blobs to the origin cluster
	// and not propagate the blobs to an external storage backend. This means the
	// blobs will not be persisted.
	DisableUploadThrough bool `yaml:"disable_upload_through"`

	// Namespace configured on origin which blobs are uploaded to.
	Namespace string `yaml:"namespace"`
}
