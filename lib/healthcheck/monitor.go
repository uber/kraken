// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package healthcheck

import (
	"sync"
	"time"

	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/utils/stringset"
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
