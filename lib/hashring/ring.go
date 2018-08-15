package hashring

import (
	"sync"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/stringset"
)

const _defaultWeight = 100

// Ring is a rendezvous hashing ring which calculates an ordered replica set
// of healthy addresses which own any given digest.
//
// Address membership within the ring is defined by a dynamic hostlist.List. On
// top of that, replica sets are filtered by the health status of their addresses.
// Membership and health status may be refreshed by using Monitor.
//
// Ring maintains the invariant that it is always non-empty and can always provide
// locations, although in some scenarios the provided locations are not guaranteed
// to be healthy (see Locations).
type Ring interface {
	Locations(d core.Digest) []string
	Contains(addr string) bool
	Monitor(stop <-chan struct{})
	Refresh() error
}

type ring struct {
	config  Config
	cluster hostlist.List
	filter  healthcheck.Filter

	mu      sync.RWMutex // Protects the following fields:
	addrs   stringset.Set
	hash    *hrw.RendezvousHash
	healthy stringset.Set
}

// New creates a new Ring whose members are defined by cluster. If no initial
// membership can be resolved, returns error.
func New(config Config, cluster hostlist.List, filter healthcheck.Filter) (Ring, error) {
	config.applyDefaults()
	r := &ring{
		config:  config,
		cluster: cluster,
		filter:  filter,
	}
	if err := r.Refresh(); err != nil {
		return nil, err
	}
	return r, nil
}

// Locations returns an ordered replica set of healthy addresses which own d.
// If all addresses in the replica set are unhealthy, then returns the next
// healthy address. If all addresses in the ring are unhealthy, then returns
// the first address which owns d (regardless of health). As such, Locations
// always returns a non-empty list.
func (r *ring) Locations(d core.Digest) []string {
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
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.addrs.Has(addr)
}

// Monitor refreshes the ring at the configured interval. Blocks until the
// provided stop channel is closed.
func (r *ring) Monitor(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-time.After(r.config.RefreshInterval):
			if err := r.Refresh(); err != nil {
				panic("invariant violation: refresh failed for initialized ring: " + err.Error())
			}
		}
	}
}

// Refresh updates the membership and health information of r.
func (r *ring) Refresh() error {
	latest, err := r.cluster.Resolve()
	if err != nil {
		if len(r.addrs) == 0 {
			return err
		}
		// We can recover from this error by continuing to use the previous
		// membership.
		log.Errorf("Error resolving latest hash ring membership: %s", err)
		latest = r.addrs
	}

	healthy := r.filter.Run(latest)

	hash := r.hash
	if !stringset.Equal(r.addrs, latest) {
		// Membership has changed -- update hash nodes.
		hash = hrw.NewRendezvousHash(hrw.Murmur3Hash, hrw.UInt64ToFloat64)
		for addr := range latest {
			hash.AddNode(addr, _defaultWeight)
		}
	}

	r.mu.Lock()
	r.addrs = latest
	r.hash = hash
	r.healthy = healthy
	r.mu.Unlock()

	return nil
}
