package upstream

import (
	"code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/andres-erbsen/clock"
)

// ActiveConfig composes host configuration for an upstream service with an
// active health check.
type ActiveConfig struct {
	Hosts       hostlist.Config         `yaml:"hosts"`
	HealthCheck ActiveHealthCheckConfig `yaml:"healthcheck"`
}

// ActiveHealthCheckConfig wraps health check configuration.
type ActiveHealthCheckConfig struct {
	Filter   healthcheck.FilterConfig  `yaml:"filter"`
	Monitor  healthcheck.MonitorConfig `yaml:"monitor"`
	Disabled bool                      `yaml:"disabled"`
}

// Build creates a healthcheck.List with built-in active health checks.
func (c ActiveConfig) Build() (healthcheck.List, error) {
	return c.BuildWithHealthChecker(healthcheck.Default(nil))
}

// BuildWithHealthChecker creates a healthcheck.List with customized health checks.
func (c ActiveConfig) BuildWithHealthChecker(checker healthcheck.Checker) (healthcheck.List, error) {
	hosts, err := hostlist.New(c.Hosts)
	if err != nil {
		return nil, err
	}
	if c.HealthCheck.Disabled {
		log.With("hosts", c.Hosts).Warn("Health checks disabled")
		return healthcheck.NoopFailed(hosts), nil
	}
	filter := healthcheck.NewFilter(c.HealthCheck.Filter, checker)
	monitor := healthcheck.NewMonitor(c.HealthCheck.Monitor, hosts, filter)
	return healthcheck.NoopFailed(monitor), nil
}

// StableAddr returns a stable address that can be advertised as the address
// for this service. If c is backed by DNS, returns the DNS record. If c is
// backed by a static list, returns a random address.
func (c ActiveConfig) StableAddr() (string, error) {
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

// PassiveConfig composes host configuruation for an upstream service with a
// passive health check.
type PassiveConfig struct {
	Hosts       hostlist.Config           `yaml:"hosts"`
	HealthCheck healthcheck.PassiveConfig `yaml:"healthcheck"`
}

// Build creates healthcheck.List enabled with passive health checks.
func (c PassiveConfig) Build() (healthcheck.List, error) {
	hosts, err := hostlist.New(c.Hosts)
	if err != nil {
		return nil, err
	}
	return healthcheck.NewPassive(c.HealthCheck, clock.New(), hosts), nil
}
