package healthcheck

import (
	"sync"

	"code.uber.internal/infra/kraken/utils/stringset"
)

// state tracks the health status of a set of hosts. In particular, it tracks
// consecutive passes or fails which cause hosts to transition between healthy
// and unhealthy.
//
// state is thread-safe.
type state struct {
	sync.Mutex
	config  FilterConfig
	all     stringset.Set
	healthy stringset.Set
	trend   map[string]int
}

func newState(config FilterConfig) *state {
	return &state{
		config:  config,
		all:     stringset.New(),
		healthy: stringset.New(),
		trend:   make(map[string]int),
	}
}

// sync sets the current state to addrs. New entries are initialized as healthy,
// while existing entries not found in addrs are removed from s.
func (s *state) sync(addrs stringset.Set) {
	s.Lock()
	defer s.Unlock()

	for addr := range addrs {
		if !s.all.Has(addr) {
			s.all.Add(addr)
			s.healthy.Add(addr)
		}
	}

	for addr := range s.healthy {
		if !addrs.Has(addr) {
			s.healthy.Remove(addr)
			delete(s.trend, addr)
		}
	}
}

// failed marks addr as failed.
func (s *state) failed(addr string) {
	s.Lock()
	defer s.Unlock()

	s.trend[addr] = max(min(s.trend[addr]-1, -1), -s.config.Fails)
	if s.trend[addr] == -s.config.Fails {
		s.healthy.Remove(addr)
	}
}

// passed marks addr as passed.
func (s *state) passed(addr string) {
	s.Lock()
	defer s.Unlock()

	s.trend[addr] = min(max(s.trend[addr]+1, 1), s.config.Passes)
	if s.trend[addr] == s.config.Passes {
		s.healthy.Add(addr)
	}
}

// getHealthy returns the current healthy hosts.
func (s *state) getHealthy() stringset.Set {
	s.Lock()
	defer s.Unlock()

	return s.healthy.Copy()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
