package hashring

import (
	"sync"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hrw"
	"github.com/uber/kraken/utils/stringset"
)

const _defaultWeight = 100

// Watcher allows clients to watch the ring for changes. Whenever membership
// changes, each registered Watcher is notified with the latest hosts.
type Watcher interface {
	Notify(latest stringset.Set)
}

// Ring is a rendezvous hashing ring which calculates an ordered replica set
// of healthy addresses which own any given digest.
//
// Address membership within the ring is defined by a dynamic healtcheck.List. On
// top of that, replica sets are filtered by the health status of their addresses.
// Membership and health status may be refreshed by healthcheck.List.
//
// Ring maintains the invariant that it is always non-empty and can always provide
// locations, although in some scenarios the provided locations are not guaranteed
// to be healthy (see Locations).
type Ring interface {
	Locations(d core.Digest) []string
	Contains(addr string) bool
	Failed(addr string)
	Refresh()
}

type ring struct {
	config  Config
	cluster healthcheck.List

	mu      sync.RWMutex // Protects the following fields:
	addrs   stringset.Set
	hash    *hrw.RendezvousHash
	healthy stringset.Set

	watchers []Watcher
}

// Option allows setting custom parameters for ring.
type Option func(*ring)

// WithWatcher adds a watcher to the ring. Can be used multiple times.
func WithWatcher(w Watcher) Option {
	return func(r *ring) { r.watchers = append(r.watchers, w) }
}

// New creates a new Ring whose members are defined by cluster.
func New(
	config Config, cluster healthcheck.List, opts ...Option) Ring {

	config.applyDefaults()
	r := &ring{
		config:  config,
		cluster: cluster,
	}
	for _, opt := range opts {
		opt(r)
	}
	r.Refresh()
	return r
}

// Locations returns an ordered replica set of healthy addresses which own d.
// If all addresses in the replica set are unhealthy, then returns the next
// healthy address. If all addresses in the ring are unhealthy, then returns
// the first address which owns d (regardless of health). As such, Locations
// always returns a non-empty list.
func (r *ring) Locations(d core.Digest) []string {
	r.Refresh()

	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := r.hash.GetOrderedNodes(d.ShardID(), len(r.addrs))
	if len(nodes) != len(r.addrs) {
		panic("invariant violation: ordered hash nodes not equal to cluster size")
	}

	if len(r.healthy) == 0 {
		return []string{nodes[0].Label}
	}

	var locs []string
	for i := 0; i < len(nodes) && (len(locs) == 0 || i < r.config.MaxReplica); i++ {
		addr := nodes[i].Label
		if r.healthy.Has(addr) {
			locs = append(locs, addr)
		}
	}
	return locs
}

// Contains returns whether the ring contains addr.
func (r *ring) Contains(addr string) bool {
	r.Refresh()

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.addrs.Has(addr)
}

// Failed marked an addr as temporarily unhealthy.
// This is used for a passive healthcheck.List.
func (r *ring) Failed(addr string) {
	r.cluster.Failed(addr)
}

// Refresh updates the membership and health information of r.
func (r *ring) Refresh() {
	healthy, all := r.cluster.Resolve()

	hash := r.hash
	if !stringset.Equal(r.addrs, all) {
		// Membership has changed -- update hash nodes.
		hash = hrw.NewRendezvousHash(hrw.Murmur3Hash, hrw.UInt64ToFloat64)
		for addr := range all {
			hash.AddNode(addr, _defaultWeight)
		}
		// Notify watchers.
		for _, w := range r.watchers {
			w.Notify(all.Copy())
		}
	}

	r.mu.Lock()
	r.addrs = all
	r.hash = hash
	r.healthy = healthy
	r.mu.Unlock()
}
