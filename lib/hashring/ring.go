package hashring

import (
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/utils/stringset"
)

const _defaultWeight = 100

// Ring is a rendezvous hashing ring which calculates an ordered list of
// addresses which own any given digest.
type Ring interface {
	Locations(d core.Digest) []string
	Contains(addr string) bool
}

type ring struct {
	config Config
	addrs  stringset.Set
	hash   *hrw.RendezvousHash
}

// New creates a new Ring containing the initial resolved contents of cluster.
func New(config Config, cluster hostlist.List) (Ring, error) {
	config.applyDefaults()
	addrs, err := cluster.Resolve()
	if err != nil {
		return nil, fmt.Errorf("hostlist: %s", err)
	}
	hash := hrw.NewRendezvousHash(hrw.Murmur3Hash, hrw.UInt64ToFloat64)
	for addr := range addrs {
		hash.AddNode(addr, _defaultWeight)
	}
	return &ring{config, addrs, hash}, nil
}

// Locations returns an ordered list of addresses which own d.
func (r *ring) Locations(d core.Digest) []string {
	nodes := r.hash.GetOrderedNodes(d.ShardID(), r.config.MaxReplica)
	var addrs []string
	for _, n := range nodes {
		addrs = append(addrs, n.Label)
	}
	return addrs
}

// Contains returns whether the ring contains addr.
func (r *ring) Contains(addr string) bool {
	return r.addrs.Has(addr)
}
