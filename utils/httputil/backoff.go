package httputil

import (
	"time"

	"github.com/cenkalti/backoff"
)

// ExponentialBackOffConfig maps backoff settings into YAML config format.
type ExponentialBackOffConfig struct {
	Enabled             bool          `yaml:"enabled"`
	InitialInterval     time.Duration `yaml:"initial_interval"`
	RandomizationFactor float64       `yaml:"randomization_factor"`
	Multiplier          float64       `yaml:"multiplier"`
	MaxInterval         time.Duration `yaml:"max_interval"`
	MaxRetries          uint64        `yaml:"max_retries"`
}

func (c *ExponentialBackOffConfig) applyDefaults() {
	if c.InitialInterval == 0 {
		c.InitialInterval = 2 * time.Second
	}
	if c.RandomizationFactor == 0 {
		c.RandomizationFactor = 0.05
	}
	if c.Multiplier == 0 {
		c.Multiplier = 2
	}
	if c.MaxInterval == 0 {
		c.MaxInterval = 30 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 5
	}
}

// Build creates a new ExponentialBackOff using c's settings (if enabled).
func (c ExponentialBackOffConfig) Build() backoff.BackOff {
	if c.Enabled {
		c.applyDefaults()
		b := &backoff.ExponentialBackOff{
			InitialInterval:     c.InitialInterval,
			RandomizationFactor: c.RandomizationFactor,
			Multiplier:          c.Multiplier,
			MaxInterval:         c.MaxInterval,
			Clock:               backoff.SystemClock,
		}
		return backoff.WithMaxRetries(b, c.MaxRetries)
	}
	return &backoff.StopBackOff{}
}
