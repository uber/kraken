package blobserver

import (
	"errors"

	"go.uber.org/atomic"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
)

// ClusterClientResolver defines an interface for accessing Clients at a cluster
// level.
type ClusterClientResolver interface {
	Resolve(d image.Digest) ([]Client, error)
}

// RoundRobinClientResolver implements ClusterClientResolver using a list of
// hosts (which may be DNS servers).
type RoundRobinClientResolver struct {
	clientProvider ClientProvider
	retries        int
	cursor         *atomic.Uint32
	hosts          []string
}

// NewRoundRobinClientResolver returns a new RoundRobinClientResolver.
func NewRoundRobinClientResolver(
	cp ClientProvider,
	retries int,
	hosts ...string) (*RoundRobinClientResolver, error) {

	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided")
	}
	return &RoundRobinClientResolver{
		clientProvider: cp,
		retries:        retries,
		cursor:         atomic.NewUint32(0),
		hosts:          hosts,
	}, nil
}

func (r *RoundRobinClientResolver) locations(d image.Digest) ([]string, error) {
	var err error
	for i := 0; i < r.retries; i++ {
		next := r.cursor.Inc() % uint32(len(r.hosts))
		server := r.hosts[next]
		var locs []string
		locs, err = r.clientProvider.Provide(server).Locations(d)
		if err == nil {
			return locs, nil
		}
	}
	return nil, err
}

// Resolve returns a list of Clients for the origin servers which own d.
func (r *RoundRobinClientResolver) Resolve(d image.Digest) ([]Client, error) {
	locs, err := r.locations(d)
	if err != nil {
		return nil, err
	}
	var clients []Client
	for _, loc := range locs {
		clients = append(clients, r.clientProvider.Provide(loc))
	}
	return clients, nil
}
