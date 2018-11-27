package healthcheck

import (
	"time"
)

// FilterConfig defines configuration for Filter.
type FilterConfig struct {
	// Fails is the number of consecutive failed health checks for a host to be
	// considered unhealthy.
	Fails int `yaml:"fails"`

	// Passes is the number of consecutive passed health checks for a host to be
	// considered healthy.
	Passes int `yaml:"passes"`

	// Timeout of each individual health check.
	Timeout time.Duration `yaml:"timeout"`
}

func (c *FilterConfig) applyDefaults() {
	if c.Fails == 0 {
		c.Fails = 3
	}
	if c.Passes == 0 {
		c.Passes = 2
	}
	if c.Timeout == 0 {
		c.Timeout = 3 * time.Second
	}
}

// MonitorConfig defines configuration for Monitor.
type MonitorConfig struct {
	Interval time.Duration `yaml:"interval"`
}

func (c *MonitorConfig) applyDefaults() {
	if c.Interval == 0 {
		c.Interval = 10 * time.Second
	}
}

// PassiveConfig defines configuration for Passive.
type PassiveConfig struct {
	// Fails is the number of failed requests that must occur during the FailTimeout
	// period for a host to be marked as unhealthy.
	Fails int `yaml:"fails"`

	// FailTimeout is the window of time during which Fails must occur for a host
	// to be marked as unhealthy.
	//
	// FailTimeout is also the time for which a server is marked unhealthy.
	FailTimeout time.Duration `yaml:"fail_timeout"`
}

func (c *PassiveConfig) applyDefaults() {
	if c.Fails == 0 {
		c.Fails = 3
	}
	if c.FailTimeout == 0 {
		c.FailTimeout = 5 * time.Minute
	}
}
