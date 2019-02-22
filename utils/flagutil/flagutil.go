package flagutil

import (
	"flag"
	"strconv"
)

// Ints allows specifying a slice of ints via flags, where multiple flags of the
// same name append to the slice.
type Ints []int

var _ flag.Value = (*Ints)(nil)

func (i *Ints) String() string {
	return "list of ints"
}

// Set appends v to i.
func (i *Ints) Set(v string) error {
	n, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	*i = append(*i, n)
	return nil
}
