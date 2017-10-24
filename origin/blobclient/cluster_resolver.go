package blobclient

import (
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/serverset"
)

// ClusterResolver defines an interface for accessing Clients at a cluster
// level.
type ClusterResolver interface {
	Resolve(d image.Digest) ([]Client, error)
}

// RoundRobinResolver implements ClusterResolver using a list of addresses
// (which may be DNS servers).
type RoundRobinResolver struct {
	provider Provider
	servers  *serverset.RoundRobin
}

// NewRoundRobinResolver returns a new RoundRobinResolver.
func NewRoundRobinResolver(
	p Provider,
	config serverset.RoundRobinConfig) (*RoundRobinResolver, error) {

	servers, err := serverset.NewRoundRobin(config)
	if err != nil {
		return nil, err
	}
	return &RoundRobinResolver{p, servers}, nil
}

// Resolve returns a list of Clients for the origin servers which own d.
func (r *RoundRobinResolver) Resolve(d image.Digest) ([]Client, error) {
	var err error
	for it := r.servers.Iter(); it.HasNext(); it.Next() {
		var locs []string
		locs, err = r.provider.Provide(it.Addr()).Locations(d)
		if err == nil {
			var clients []Client
			for _, loc := range locs {
				clients = append(clients, r.provider.Provide(loc))
			}
			return clients, nil
		}
	}
	return nil, err
}
