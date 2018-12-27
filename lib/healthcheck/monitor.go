package healthcheck

import (
	"sync"

	"github.com/andres-erbsen/clock"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/utils/stringset"
)

// Monitor performs active health checks asynchronously. Can be used in
// as a hostlist.List.
type Monitor struct {
	config MonitorConfig
	clk    clock.Clock
	hosts  hostlist.List
	filter Filter

	mu      sync.RWMutex
	all     stringset.Set
	healthy stringset.Set

	stop chan struct{}
}

var _ List = (*Monitor)(nil)

// Option allows setting custom parameters for Monitor.
type Option func(*Monitor)

// WithClk set Monitor's clock.
func WithClk(clk clock.Clock) Option {
	return func(r *Monitor) { r.clk = clk }
}

// NewMonitor monitors the health of hosts using filter.
func NewMonitor(config MonitorConfig, hosts hostlist.List, filter Filter, opts ...Option) *Monitor {
	config.applyDefaults()
	all := hosts.Resolve()
	m := &Monitor{
		config:  config,
		clk:     clock.New(),
		hosts:   hosts,
		filter:  filter,
		all:     all,
		healthy: all,
		stop:    make(chan struct{}),
	}

	for _, opt := range opts {
		opt(m)
	}

	go m.loop()
	return m
}

// Resolve returns the latest healthy hosts and all hosts.
func (m *Monitor) Resolve() (stringset.Set, stringset.Set) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.healthy.Copy(), m.all.Copy()
}

// Failed is noop for Monitor.
func (m *Monitor) Failed(addr string) {}

// Stop stops the monitor.
func (m *Monitor) Stop() {
	close(m.stop)
}

func (m *Monitor) loop() {
	for {
		select {
		case <-m.stop:
			return
		case <-m.clk.Tick(m.config.Interval):
			all := m.hosts.Resolve()
			healthy := m.filter.Run(all)
			m.mu.Lock()
			m.all = all
			m.healthy = healthy
			m.mu.Unlock()
		}
	}
}
