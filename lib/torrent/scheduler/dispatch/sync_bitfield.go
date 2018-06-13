package dispatch

import (
	"bytes"
	"sync"

	"github.com/willf/bitset"
)

type syncBitfield struct {
	sync.RWMutex
	b *bitset.BitSet
}

func newSyncBitfield(b *bitset.BitSet) *syncBitfield {
	return &syncBitfield{
		b: b.Clone(),
	}
}

func (s *syncBitfield) Intersection(other *bitset.BitSet) *bitset.BitSet {
	s.RLock()
	defer s.RUnlock()

	return s.b.Intersection(other)
}

func (s *syncBitfield) Len() uint {
	s.RLock()
	defer s.RUnlock()

	return s.b.Len()
}

func (s *syncBitfield) Has(i uint) bool {
	s.RLock()
	defer s.RUnlock()

	return s.b.Test(i)
}

func (s *syncBitfield) Complete() bool {
	s.RLock()
	defer s.RUnlock()

	return s.b.All()
}

func (s *syncBitfield) Set(i uint, v bool) {
	s.Lock()
	defer s.Unlock()

	s.b.SetTo(i, v)
}

// GetAllSet returns the indices of all set bits in the bitset.
func (s *syncBitfield) GetAllSet() []uint {
	s.RLock()
	defer s.RUnlock()

	all := make([]uint, 0, s.b.Len())
	buffer := make([]uint, s.b.Len())
	j := uint(0)
	j, buffer = s.b.NextSetMany(j, buffer)
	for ; len(buffer) > 0; j, buffer = s.b.NextSetMany(j, buffer) {
		all = append(all, buffer...)
		j++
	}
	return all
}

func (s *syncBitfield) SetAll(v bool) {
	s.Lock()
	defer s.Unlock()

	for i := uint(0); i < s.b.Len(); i++ {
		s.b.SetTo(i, v)
	}
}

func (s *syncBitfield) String() string {
	s.RLock()
	defer s.RUnlock()

	var buf bytes.Buffer
	for i := uint(0); i < s.b.Len(); i++ {
		if s.b.Test(i) {
			buf.WriteString("1")
		} else {
			buf.WriteString("0")
		}
	}
	return buf.String()
}
