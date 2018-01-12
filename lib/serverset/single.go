package serverset

import "errors"

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
	err  error
}

// Addr implements Iter.Addr
func (it *SingleIter) Addr() string { return it.addr }

// Next implements Iter.Next
func (it *SingleIter) Next() bool {
	if it.done {
		it.err = errors.New("single iteration reached")
		return false
	}
	it.done = true
	return true
}

// Err returns error if iteration has ended.
func (it *SingleIter) Err() error {
	return it.err
}

// Iter returns a new Single iterator.
func (s *Single) Iter() Iter {
	return &SingleIter{s.addr, false, nil}
}
