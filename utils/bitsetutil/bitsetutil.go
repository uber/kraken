package bitsetutil

import "github.com/willf/bitset"

// FromBools returns a new BitSet from the given bools.
func FromBools(bs ...bool) *bitset.BitSet {
	s := bitset.New(uint(len(bs)))
	for i, b := range bs {
		s.SetTo(uint(i), b)
	}
	return s
}
