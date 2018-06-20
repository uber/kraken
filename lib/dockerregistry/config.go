package dockerregistry

import (
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry"
	"github.com/uber-go/tally"
)

const (
	_proxy = "proxy"
	_agent = "agent"
)

// Config defines registry configuration.
type Config struct {
	Docker configuration.Configuration `yaml:"docker"`
}

// ProxyParameters builds proxy-specific parameters.
func (c Config) ProxyParameters(
	transferer transfer.ImageTransferer,
	cas *store.CAStore,
	metrics tally.Scope) configuration.Parameters {

	return configuration.Parameters{
		"component":  _proxy,
		"config":     c,
		"transferer": transferer,
		"castore":    cas,
		"metrics":    metrics,
	}
}

// AgentParameters builds agent-specific parameters.
func (c Config) AgentParameters(
	transferer transfer.ImageTransferer,
	bs BlobStore,
	metrics tally.Scope) configuration.Parameters {

	return configuration.Parameters{
		"component":  _agent,
		"config":     c,
		"transferer": transferer,
		"blobstore":  bs,
		"metrics":    metrics,
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
