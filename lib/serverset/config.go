package serverset

import "errors"

// RoundRobinConfig defines roundrobin configuration.
type RoundRobinConfig struct {
	Retries int      `yaml:"retries"`
	Addrs   []string `yaml:"addrs"`
}

func (c RoundRobinConfig) applyDefaults() (RoundRobinConfig, error) {
	if len(c.Addrs) == 0 {
		return c, errors.New("no addrs provided")
	}
	if c.Retries == 0 {
		c.Retries = 3
	}
	return c, nil
}
