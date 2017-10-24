package serverset

// Single defines a set which iterates over a single server once. Useful for
// testing purposes.
type Single struct {
	addr string
}

// NewSingle returns a new Single set.
func NewSingle(addr string) *Single {
	return &Single{addr}
}

// SingleIter defines a Single iterator.
type SingleIter struct {
	addr string
	done bool
}

// Addr implements Iter.Addr
func (it *SingleIter) Addr() string { return it.addr }

// HasNext implements Iter.HasNext
func (it *SingleIter) HasNext() bool { return !it.done }

// Next implements Iter.Next
func (it *SingleIter) Next() { it.done = true }

// Iter returns a new Single iterator.
func (s *Single) Iter() Iter {
	return &SingleIter{s.addr, false}
}
