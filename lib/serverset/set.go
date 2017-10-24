package serverset

// Set defines an interface for accessing a set of servers via iterator.
type Set interface {
	Iter() Iter
}

// Iter defines an iterator over a set of servers.
type Iter interface {
	// Addr returns the current address of the iteration.
	Addr() string

	// HasNext returns whether the iterator may advance.
	HasNext() bool

	// Next advances the iterator to the next address.
	Next()
}
