package dockerregistry

import (
	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/lib/store"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry"
	"github.com/uber-go/tally"
)

const (
	_rw = "rw"
	_ro = "ro"
)

// Config defines registry configuration.
type Config struct {
	Docker configuration.Configuration `yaml:"docker"`
}

// ReadWriteParameters builds parameters for a read-write driver.
func (c Config) ReadWriteParameters(
	transferer transfer.ImageTransferer,
	cas *store.CAStore,
	metrics tally.Scope) configuration.Parameters {

	return configuration.Parameters{
		"constructor": _rw,
		"config":      c,
		"transferer":  transferer,
		"castore":     cas,
		"metrics":     metrics,
	}
}

// ReadOnlyParameters builds parameters for a read-only driver.
func (c Config) ReadOnlyParameters(
	transferer transfer.ImageTransferer,
	bs BlobStore,
	metrics tally.Scope) configuration.Parameters {

	return configuration.Parameters{
		"constructor": _ro,
		"config":      c,
		"transferer":  transferer,
		"blobstore":   bs,
		"metrics":     metrics,
	}
}

// Build builds a new docker registry.
func (c Config) Build(parameters configuration.Parameters) (*registry.Registry, error) {
	c.Docker.Storage = configuration.Storage{
		Name: parameters,
		// Redirect is enabled by default in docker registry.
		// We implement redirect on proxy level so we do not need this in storage driver for now.
		"redirect": configuration.Parameters{
			"disable": true,
		},
	}
	return registry.NewRegistry(context.Background(), &c.Docker)
}
