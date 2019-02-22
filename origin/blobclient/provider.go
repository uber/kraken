package blobclient

import (
	"github.com/uber/kraken/lib/hostlist"
)

// Provider defines an interface for creating Client scoped to an origin addr.
type Provider interface {
	Provide(addr string) Client
}

// HTTPProvider provides HTTPClients.
type HTTPProvider struct {
	opts []Option
}

// NewProvider returns a new HTTPProvider.
func NewProvider(opts ...Option) HTTPProvider {
	return HTTPProvider{opts}
}

// Provide implements ClientProvider's Provide.
// TODO(codyg): Make this return error.
func (p HTTPProvider) Provide(addr string) Client {
	return New(addr, p.opts...)
}

// ClusterProvider creates ClusterClients from dns records.
type ClusterProvider interface {
	Provide(dns string) (ClusterClient, error)
}

// HTTPClusterProvider provides ClusterClients backed by HTTP. Does not include
// health checks.
type HTTPClusterProvider struct {
	opts []Option
}

// NewClusterProvider returns a new HTTPClusterProvider.
func NewClusterProvider(opts ...Option) HTTPClusterProvider {
	return HTTPClusterProvider{opts}
}

// Provide creates a new ClusterClient.
func (p HTTPClusterProvider) Provide(dns string) (ClusterClient, error) {
	hosts, err := hostlist.New(hostlist.Config{DNS: dns})
	if err != nil {
		return nil, err
	}
	return NewClusterClient(NewClientResolver(NewProvider(p.opts...), hosts)), nil
}
