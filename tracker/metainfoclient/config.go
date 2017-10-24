package metainfoclient

import "time"

// Config defines Client configuration.
type Config struct {
	Timeout time.Duration `yaml:"timeout"`
}

func (c Config) applyDefaults() Config {
	if c.Timeout == 0 {
		c.Timeout = 30 * time.Second
	}
	return c
}
