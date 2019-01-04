package healthcheck

import (
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/utils/stringset"
)

// Passive wraps a passive health check and can be used as a hostlist.List. See
// PassiveFilter for passive health check documenation.
type Passive struct {
	hosts  hostlist.List
	filter PassiveFilter
}

// NewPassive returns a new Passive.
func NewPassive(hosts hostlist.List, filter PassiveFilter) *Passive {
	return &Passive{hosts, filter}
}

// Resolve returns the latest healthy hosts. If all hosts are unhealthy, returns
// all hosts.
func (p *Passive) Resolve() stringset.Set {
	all := p.hosts.Resolve()
	healthy := p.filter.Run(all)
	if len(healthy) == 0 {
		return all
	}
	return healthy
}

// Failed marks a request to addr as failed.
func (p *Passive) Failed(addr string) {
	p.filter.Failed(addr)
}
