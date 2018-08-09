package healthcheck

import (
	"time"

	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/stringset"
)

// Monitor asynchronoulsy runs a Filter at a given interval.
type Monitor interface {
	GetHealthy() stringset.Set
	Stop()
}

type monitor struct {
	config MonitorConfig
	filter Filter
	stop   chan struct{}
}

// NewMonitor creates and starts a new Monitor.
func NewMonitor(config MonitorConfig, filter Filter) (Monitor, error) {
	config.applyDefaults()
	if err := filter.Init(); err != nil {
		return nil, err
	}
	m := &monitor{config, filter, make(chan struct{})}
	go m.loop()
	return m, nil
}

// GetHealthy returns the latest healthy hosts.
func (m *monitor) GetHealthy() stringset.Set {
	return m.filter.GetHealthy()
}

// Stop stops m. Must be called at most once. GetHealthy will continue to
// report the last set of healthy hosts.
func (m *monitor) Stop() {
	close(m.stop)
}

func (m *monitor) loop() {
	for {
		select {
		case <-m.stop:
			return
		case <-time.After(m.config.Interval):
			if err := m.filter.Run(); err != nil {
				log.Errorf("Error running health checks: %s", err)
			}
		}
	}
}
