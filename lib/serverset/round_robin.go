package serverset

import "go.uber.org/atomic"

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

// RoundRobinIter defines an iterator over addresses.
type RoundRobinIter struct {
	cursor  *atomic.Uint32
	addrs   []string
	retries int
	i       int
}

// Addr implements Iter.Addr
func (it *RoundRobinIter) Addr() string { return it.addrs[it.i] }

// HasNext implements Iter.HasNext
func (it *RoundRobinIter) HasNext() bool { return it.retries > 0 }

// Next implements Iter.Next
func (it *RoundRobinIter) Next() {
	it.i = int(it.cursor.Inc() % uint32(len(it.addrs)))
	it.retries--
}

// Iter returns an iterator over the configured addresses.
func (r *RoundRobin) Iter() Iter {
	it := &RoundRobinIter{
		cursor:  r.cursor,
		addrs:   r.config.Addrs,
		retries: r.config.Retries + 1,
	}
	// Kick off the iterator.
	it.Next()
	return it
}
