package scheduler

import (
	"sync"

	"code.uber.internal/infra/kraken/lib/torrent/storage"
)

type syncBitfield struct {
	sync.RWMutex
	s           storage.Bitfield
	numComplete int
}

func newSyncBitfield(s []bool) *syncBitfield {
	var numComplete int
	t := make([]bool, len(s))
	for i, v := range s {
		if v {
			numComplete++
		}
		t[i] = v
	}
	return &syncBitfield{
		s:           t,
		numComplete: numComplete,
	}
}

func (b *syncBitfield) Has(i int) bool {
	b.RLock()
	defer b.RUnlock()

	return b.s[i]
}

func (b *syncBitfield) Set(i int, v bool) {
	b.Lock()
	defer b.Unlock()

	if !b.s[i] && v { // false -> true
		b.numComplete++
	} else if b.s[i] && !v { // true -> false
		b.numComplete--
	}
	b.s[i] = v
}

func (b *syncBitfield) Complete() bool {
	b.Lock()
	defer b.Unlock()

	return b.numComplete == len(b.s)
}

func (b *syncBitfield) String() string {
	b.RLock()
	defer b.RUnlock()

	return b.s.String()
}
