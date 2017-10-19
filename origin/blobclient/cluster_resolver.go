package blobclient

import (
	"errors"

	"go.uber.org/atomic"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
)

// ClusterResolver defines an interface for accessing Clients at a cluster
// level.
type ClusterResolver interface {
	Resolve(d image.Digest) ([]Client, error)
}

// RoundRobinResolver implements ClusterResolver using a list of
// hosts (which may be DNS servers).
type RoundRobinResolver struct {
	provider Provider
	retries  int
	cursor   *atomic.Uint32
	hosts    []string
}

// NewRoundRobinResolver returns a new RoundRobinResolver.
func NewRoundRobinResolver(
	p Provider,
	retries int,
	hosts ...string) (*RoundRobinResolver, error) {

	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided")
	}
	return &RoundRobinResolver{
		provider: p,
		retries:  retries,
		cursor:   atomic.NewUint32(0),
		hosts:    hosts,
	}, nil
}

func (r *RoundRobinResolver) locations(d image.Digest) ([]string, error) {
	var err error
	for i := 0; i < r.retries; i++ {
		next := r.cursor.Inc() % uint32(len(r.hosts))
		server := r.hosts[next]
		var locs []string
		locs, err = r.provider.Provide(server).Locations(d)
		if err == nil {
			return locs, nil
		}
	}
	return nil, err
}

// Resolve returns a list of Clients for the origin servers which own d.
func (r *RoundRobinResolver) Resolve(d image.Digest) ([]Client, error) {
	locs, err := r.locations(d)
	if err != nil {
		return nil, err
	}
	var clients []Client
	for _, loc := range locs {
		clients = append(clients, r.provider.Provide(loc))
	}
	return clients, nil
}
