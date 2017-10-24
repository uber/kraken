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

// clusterResolver implements ClusterResolver.
type clusterResolver struct {
	provider Provider
	servers  serverset.Set
}

// NewClusterResolver returns a new clusterResolver.
func NewClusterResolver(p Provider, servers serverset.Set) ClusterResolver {
	return &clusterResolver{p, servers}
}

// Resolve returns a list of Clients for the origin servers which own d.
func (r *clusterResolver) Resolve(d image.Digest) ([]Client, error) {
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
