package upstream

import (
	"code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/utils/log"
)

// Config composes configuration for an upstream service. This combines a host list
// with an optional active health check.
//
// XXX: Do not use active health checks from agents!
type Config struct {
	Hosts       hostlist.Config   `yaml:"hosts"`
	HealthCheck HealthCheckConfig `yaml:"healthcheck"`
}

// HealthCheckConfig wraps health check configuration.
type HealthCheckConfig struct {
	Filter   healthcheck.FilterConfig  `yaml:"filter"`
	Monitor  healthcheck.MonitorConfig `yaml:"monitor"`
	Disabled bool                      `yaml:"disabled"`
}

// Build creates a healthcheck.List for port with built-in active health checks.
func (c Config) Build() (healthcheck.List, error) {
	hosts, err := hostlist.New(c.Hosts)
	if err != nil {
		return nil, err
	}
	if c.HealthCheck.Disabled {
		log.With("hosts", c.Hosts).Warn("Health checks disabled")
		return healthcheck.NoopFailed(hosts), nil
	}
	filter := healthcheck.NewFilter(c.HealthCheck.Filter, healthcheck.Default())
	monitor := healthcheck.NewMonitor(c.HealthCheck.Monitor, hosts, filter)
	return healthcheck.NoopFailed(monitor), nil
}

// StableAddr returns a stable address that can be advertised as the address
// for this service. If c is backed by DNS, returns the DNS record. If c is
// backed by a static list, returns a random address.
func (c Config) StableAddr() (string, error) {
	if c.Hosts.DNS != "" {
		return c.Hosts.DNS, nil
	}
	hosts, err := hostlist.New(c.Hosts)
	if err != nil {
		return "", err
	}
	addr, err := hosts.Resolve().Random()
	if err != nil {
		panic("invariant violation: " + err.Error())
	}
	return addr, nil
}
