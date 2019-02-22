package healthcheck

import (
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber/kraken/utils/stringset"
)

// PassiveFilter filters unhealthy hosts passively by tracking failed
// requests to hosts. Clients are responsible for marking failures from
// individual hosts, and PassiveFilter updates which hosts are unhealthy. It is
// recommended that clients only mark failures for network errors, not HTTP
// errors.
type PassiveFilter interface {
	Filter
	Failed(addr string)
}

type passiveFilter struct {
	sync.Mutex
	config    PassiveFilterConfig
	clk       clock.Clock
	unhealthy map[string]time.Time
	failures  map[string][]time.Time
}

// NewPassiveFilter creates a new PassiveFilter.
func NewPassiveFilter(config PassiveFilterConfig, clk clock.Clock) PassiveFilter {
	config.applyDefaults()
	return &passiveFilter{
		config:    config,
		clk:       clk,
		unhealthy: make(map[string]time.Time),
		failures:  make(map[string][]time.Time),
	}
}

// Run removes any unhealthy from addrs.
func (f *passiveFilter) Run(addrs stringset.Set) stringset.Set {
	f.Lock()
	defer f.Unlock()

	healthy := addrs.Copy()

	for addr, t := range f.unhealthy {
		if f.clk.Now().Sub(t) > f.config.FailTimeout {
			delete(f.unhealthy, addr)
		} else {
			healthy.Remove(addr)
		}
	}

	return healthy
}

// Failed marks a request to addr as failed.
func (f *passiveFilter) Failed(addr string) {
	f.Lock()
	defer f.Unlock()

	now := f.clk.Now()

	failures := f.failures[addr]

	// Pop off the expired failures.
	for len(failures) > 0 {
		if now.Sub(failures[0]) > f.config.FailTimeout {
			failures = failures[1:]
			continue
		}
		break
	}

	// Add latest failure.
	failures = append(failures, now)

	if len(failures) >= f.config.Fails {
		f.unhealthy[addr] = now
	}
	f.failures[addr] = failures
}
