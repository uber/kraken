package blobclient

import (
	"errors"
	"math/rand"
	"time"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/serverset"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

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
			if len(locs) == 0 {
				return nil, errors.New("no locations found")
			}
			var clients []Client
			for _, loc := range locs {
				clients = append(clients, r.provider.Provide(loc))
			}
			return clients, nil
		}
	}
	return nil, err
}

// Shuffle shuffles cs in place. Useful when looping through clients in a
// randomized round-robin fashion.
func Shuffle(cs []Client) {
	for i := range cs {
		j := rand.Intn(i + 1)
		cs[i], cs[j] = cs[j], cs[i]
	}
}
