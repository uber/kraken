package scheduler

import "sync"

type syncBitfield struct {
	sync.RWMutex
	s []bool
}

func newSyncBitfield(s []bool) *syncBitfield {
	b := &syncBitfield{
		s: make([]bool, len(s)),
	}
	copy(b.s, s)
	return b
}

func (b *syncBitfield) Has(i int) bool {
	b.RLock()
	defer b.RUnlock()

	return b.s[i]
}

func (b *syncBitfield) Set(i int, v bool) {
	b.Lock()
	defer b.Unlock()

	b.s[i] = v
}

func (b *syncBitfield) String() string {
	b.RLock()
	defer b.RUnlock()

	return formatBitfield(b.s)
}
