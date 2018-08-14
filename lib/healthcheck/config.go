package healthcheck

import "time"

// Config defines configuration for Filter.
type Config struct {
	// Fails is the number of consecutive failed health checks for a host to be
	// considered unhealthy.
	Fails int `yaml:"fails"`

	// Passes is the number of consecutive passed health checks for a host to be
	// considered healthy.
	Passes int `yaml:"passes"`

	// Timeout of each individual health check.
	Timeout time.Duration `yaml:"timeout"`
}

func (c *Config) applyDefaults() {
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
