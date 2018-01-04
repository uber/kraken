// Package backoff is a configuration wrapper around an existing 3rd-party backoff library.
package backoff

import (
	"time"

	"github.com/jpillora/backoff"
)

// Config defines backoff configuration.
type Config struct {
	Min    time.Duration `yaml:"min"`
	Max    time.Duration `yaml:"max"`
	Factor float64       `yaml:"factor"`
}

func (c Config) applyDefaults() Config {
	if c.Min == 0 {
		c.Min = 1 * time.Second
	}
	if c.Max == 0 {
		c.Max = 1 * time.Minute
	}
	if c.Factor == 0 {
		c.Factor = 2
	}
	return c
}

// Backoff provides thread-safe backoff duration calculation.
type Backoff struct {
	b *backoff.Backoff
}

// New creates a new Backoff.
func New(config Config) *Backoff {
	config = config.applyDefaults()

	return &Backoff{&backoff.Backoff{
		Factor: config.Factor,
		Jitter: true,
		Min:    config.Min,
		Max:    config.Max,
	}}
}

// Duration maps an attempt number into the duration the caller should
// wait for. Attempts should always start at 0.
func (b *Backoff) Duration(attempt int) time.Duration {
	return b.b.ForAttempt(float64(attempt))
}
