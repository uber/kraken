package syncutil

import "sync"

// counter is an element in a Counters struct, wrapping a count and its lock.
type counter struct {
	mu    sync.RWMutex
	count int
}

// Counters provides a wrapper to a list of counters that supports
// concurrent update-only operations.
type Counters []counter

// NewCounters returns an initialized Counters of the given length.
func NewCounters(length int) Counters {
	return Counters(make([]counter, length))
}

// Len returns the number of counters in the Counters.
func (c Counters) Len() int {
	return len(c)
}

// Get returns the count of the counter at index i.
func (c Counters) Get(i int) int {
	c[i].mu.RLock()
	defer c[i].mu.RUnlock()

	return c[i].count
}

// Set sets the count of the counter at index i to count v.
func (c Counters) Set(i, v int) {
	c[i].mu.Lock()
	defer c[i].mu.Unlock()

	c[i].count = v
}

// Increment increments the count of the counter at index i.
func (c Counters) Increment(i int) {
	c[i].mu.Lock()
	defer c[i].mu.Unlock()

	c[i].count++
}

// Decrement decrements the count of the counter at index i.
func (c Counters) Decrement(i int) {
	c[i].mu.Lock()
	defer c[i].mu.Unlock()

	c[i].count--
}
