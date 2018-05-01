package serverset

import (
	"fmt"

	"go.uber.org/atomic"
)

// RoundRobin defines a thread-safe round-robin procedure for selecting host addresses.
// Suitable for both DNS retry (configure with single address) or a list of hosts.
type RoundRobin struct {
	config RoundRobinConfig
	cursor *atomic.Uint32
}

// NewRoundRobin creates a new RoundRobin instance.
func NewRoundRobin(config RoundRobinConfig) (*RoundRobin, error) {
	config, err := config.applyDefaults()
	if err != nil {
		return nil, err
	}
	return &RoundRobin{
		config: config,
		cursor: atomic.NewUint32(0),
	}, nil
}

// MustRoundRobin creates a new RoundRobin instance for addrs, panicking if
// addrs is empty. Useful for testing.
func MustRoundRobin(addrs ...string) *RoundRobin {
	config := RoundRobinConfig{
		Addrs:   addrs,
		Retries: len(addrs),
	}
	rr, err := NewRoundRobin(config)
	if err != nil {
		panic(err)
	}
	return rr
}

// DNSRoundRobin creates a new RoundRobin instance for a single dns record
// with 3 retries.
func DNSRoundRobin(dns string) *RoundRobin {
	rr, err := NewRoundRobin(RoundRobinConfig{
		Addrs:   []string{dns},
		Retries: 3,
	})
	if err != nil {
		panic(err)
	}
	return rr
}

// RoundRobinIter defines an iterator over addresses.
type RoundRobinIter struct {
	cursor  *atomic.Uint32
	addrs   []string
	max     int
	retries int
	i       int
	err     error
}

// MaxRoundRobinRetryError occurs when the max number of retries was reached.
type MaxRoundRobinRetryError struct {
	max int
}

func (e MaxRoundRobinRetryError) Error() string {
	return fmt.Sprintf("round robin reached %d max retries", e.max)
}

// Addr returns the current address.
func (it *RoundRobinIter) Addr() string { return it.addrs[it.i] }

// Next advances to the next server, which may be the same server. Returns
// false if reached max retries.
func (it *RoundRobinIter) Next() bool {
	if it.retries <= 0 {
		it.err = MaxRoundRobinRetryError{it.max}
		return false
	}
	it.i = int(it.cursor.Inc() % uint32(len(it.addrs)))
	it.retries--
	return true
}

// Err returns an error if max retries was reached.
func (it *RoundRobinIter) Err() error {
	return it.err
}

// Iter returns an iterator over the configured addresses.
func (r *RoundRobin) Iter() Iter {
	it := &RoundRobinIter{
		cursor:  r.cursor,
		addrs:   r.config.Addrs,
		max:     r.config.Retries,
		retries: r.config.Retries,
	}
	return it
}
