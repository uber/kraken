// Package backoff is a configuration wrapper around an existing 3rd-party backoff library.
package backoff

import (
	"fmt"
	"time"

	"github.com/jpillora/backoff"
)

// Config defines backoff configuration.
type Config struct {
	Min          time.Duration `yaml:"min"`
	Max          time.Duration `yaml:"max"`
	Factor       float64       `yaml:"factor"`
	RetryTimeout time.Duration `yaml:"retry_timeout"`
	NoJitter     bool          `yaml:"no_jitter"`
}

func (c Config) applyDefaults() Config {
	if c.Min == 0 {
		c.Min = 1 * time.Second
	}
	if c.Max == 0 {
		c.Max = 5 * time.Second
	}
	if c.Factor == 0 {
		c.Factor = 1.3
	}
	if c.RetryTimeout == 0 {
		c.RetryTimeout = 15 * time.Minute
	}
	return c
}

// Backoff provides thread-safe backoff duration calculation.
type Backoff struct {
	config Config
	b      *backoff.Backoff
}

// New creates a new Backoff.
func New(config Config) *Backoff {
	config = config.applyDefaults()

	return &Backoff{config, &backoff.Backoff{
		Factor: config.Factor,
		Jitter: !config.NoJitter,
		Min:    config.Min,
		Max:    config.Max,
	}}
}

// Duration maps an attempt number into the duration the caller should
// wait for. Attempts should always start at 0.
func (b *Backoff) Duration(attempt int) time.Duration {
	return b.b.ForAttempt(float64(attempt))
}

// Attempts returns a Attempts iterator.
func (b *Backoff) Attempts() *Attempts {
	// Simulate the attempts to determine the max attempts within the timeout.
	attempt := -1
	var d time.Duration
	for d < b.config.RetryTimeout {
		d += b.Duration(attempt)
		attempt++
	}
	return &Attempts{
		backoff:     b,
		attempt:     -1,
		maxAttempts: attempt,
	}
}

type timeoutError struct {
	attempts int
	timeout  time.Duration
}

func (e timeoutError) Error() string {
	return fmt.Sprintf("timed out after %d attempts in %s", e.attempts, e.timeout)
}

// IsTimeoutError returns true if err occured due to Attempts timeout.
func IsTimeoutError(err error) bool {
	_, ok := err.(timeoutError)
	return ok
}

// Attempts defines an iterator for retrying some action with backoff until a
// timeout expires.
type Attempts struct {
	backoff     *Backoff
	attempt     int
	maxAttempts int
	err         error
}

// WaitForNext sleeps until the next attempt is ready to perform. Returns false if
// no more attempts can be performed due to timeout. The first call to WaitForNext
// will always return true immediately.
func (a *Attempts) WaitForNext() bool {
	if a.attempt < 0 {
		// -1 primes the first attempt, which should return immediately.
		a.attempt = 0
		return true
	}
	if a.attempt >= a.maxAttempts {
		a.err = timeoutError{a.maxAttempts, a.backoff.config.RetryTimeout}
		return false
	}
	time.Sleep(a.backoff.Duration(a.attempt))
	a.attempt++
	return true
}

// Err returns an error if a timed out.
func (a *Attempts) Err() error {
	return a.err
}
