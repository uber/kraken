package healthcheck

import (
	"sync"
	"time"

	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/utils/stringset"
)

// Monitor performs active health checks asynchronously. Can be used in
// as a hostlist.List.
type Monitor struct {
	config MonitorConfig
	hosts  hostlist.List
	filter Filter

	mu      sync.RWMutex
	healthy stringset.Set

	stop chan struct{}
}

var _ hostlist.List = (*Monitor)(nil)

// NewMonitor monitors the health of hosts using filter.
func NewMonitor(config MonitorConfig, hosts hostlist.List, filter Filter) *Monitor {
	config.applyDefaults()
	m := &Monitor{
		config:  config,
		hosts:   hosts,
		filter:  filter,
		healthy: hosts.Resolve(),
		stop:    make(chan struct{}),
	}
	go m.loop()
	return m
}

// Resolve returns the latest healthy hosts.
func (m *Monitor) Resolve() stringset.Set {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.healthy
}

// Stop stops the monitor.
func (m *Monitor) Stop() {
	close(m.stop)
}

func (m *Monitor) loop() {
	for {
		select {
		case <-m.stop:
			return
		case <-time.After(m.config.Interval):
			healthy := m.filter.Run(m.hosts.Resolve())
			m.mu.Lock()
			m.healthy = healthy
			m.mu.Unlock()
		}
	}
}
