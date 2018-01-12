package serverset

// Set defines an interface for accessing a set of servers via iterator.
type Set interface {
	Iter() Iter
}

// Iter defines an iterator over a set of servers.
type Iter interface {
	// Next advances the iterator to the next address, or returns false if iteration
	// has stopped.
	Next() bool

	// Addr returns the current address of the iteration.
	Addr() string

	// Err returns an error if iteration has stopped.
	Err() error
}
