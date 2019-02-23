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
package upstream

import (
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/utils/log"

	"github.com/andres-erbsen/clock"
)

// ActiveConfig composes host configuration for an upstream service with an
// active health check.
type ActiveConfig struct {
	Hosts       hostlist.Config         `yaml:"hosts"`
	HealthCheck ActiveHealthCheckConfig `yaml:"healthcheck"`

	checker healthcheck.Checker
}

// ActiveHealthCheckConfig wraps health check configuration.
type ActiveHealthCheckConfig struct {
	Filter   healthcheck.FilterConfig  `yaml:"filter"`
	Monitor  healthcheck.MonitorConfig `yaml:"monitor"`
	Disabled bool                      `yaml:"disabled"`
}

// ActiveOption allows setting optional ActiveConfig parameters.
type ActiveOption func(*ActiveConfig)

// WithHealthCheck configures ActiveConfig with a custom health check.
func WithHealthCheck(checker healthcheck.Checker) ActiveOption {
	return func(c *ActiveConfig) { c.checker = checker }
}

// Build creates a healthcheck.List with built-in active health checks.
func (c ActiveConfig) Build(opts ...ActiveOption) (healthcheck.List, error) {
	hosts, err := hostlist.New(c.Hosts)
	if err != nil {
		return nil, err
	}
	if c.HealthCheck.Disabled {
		log.With("hosts", c.Hosts).Warn("Health checks disabled")
		return healthcheck.NoopFailed(hosts), nil
	}
	c.checker = healthcheck.Default(nil)
	for _, opt := range opts {
		opt(&c)
	}
	filter := healthcheck.NewFilter(c.HealthCheck.Filter, c.checker)
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
	Hosts       hostlist.Config                 `yaml:"hosts"`
	HealthCheck healthcheck.PassiveFilterConfig `yaml:"healthcheck"`
}

// Build creates healthcheck.List enabled with passive health checks.
func (c PassiveConfig) Build() (healthcheck.List, error) {
	hosts, err := hostlist.New(c.Hosts)
	if err != nil {
		return nil, err
	}
	f := healthcheck.NewPassiveFilter(c.HealthCheck, clock.New())
	return healthcheck.NewPassive(hosts, f), nil
}

// PassiveRingConfig composes host configuration for an upstream service with
// a passively health checked hash ring.
type PassiveHashRingConfig struct {
	Hosts       hostlist.Config                 `yaml:"hosts"`
	HealthCheck healthcheck.PassiveFilterConfig `yaml:"healthcheck"`
	HashRing    hashring.Config                 `yaml:"hashring"`
}

// Build creates a hashring.PassiveRing.
func (c PassiveHashRingConfig) Build() (hashring.PassiveRing, error) {
	hosts, err := hostlist.New(c.Hosts)
	if err != nil {
		return nil, err
	}
	f := healthcheck.NewPassiveFilter(c.HealthCheck, clock.New())
	return hashring.NewPassive(c.HashRing, hosts, f), nil
}
