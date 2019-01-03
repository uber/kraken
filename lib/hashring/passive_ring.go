package hashring

import (
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
)

// PassiveRing is a wrapper around Ring which supports passive health checks.
// See healthcheck.PassiveFilter for passive health check documentation.
type PassiveRing interface {
	Ring
	Failed(addr string)
}

type passiveRing struct {
	Ring
	passiveFilter healthcheck.PassiveFilter
}

// NewPassive creats a new PassiveRing.
func NewPassive(
	config Config,
	cluster hostlist.List,
	passiveFilter healthcheck.PassiveFilter,
	opts ...Option) PassiveRing {

	return &passiveRing{
		New(config, cluster, passiveFilter, opts...),
		passiveFilter,
	}
}

// Failed marks a request to addr as failed.
func (p *passiveRing) Failed(addr string) {
	p.passiveFilter.Failed(addr)
}
