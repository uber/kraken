package storage

import (
	"sync"

	"code.uber.internal/infra/kraken/torlib"
)

type testMetaInfoStore struct {
	sync.Mutex
	metainfo map[string]*torlib.MetaInfo
}

// TestMetaInfoStore returns a thread-safe, in-memory metainfo store for testing
// purposes.
func TestMetaInfoStore() MetaInfoStore {
	return &testMetaInfoStore{
		metainfo: make(map[string]*torlib.MetaInfo),
	}
}

func (s *testMetaInfoStore) GetMetaInfo(name string) ([]byte, error) {
	s.Lock()
	defer s.Unlock()

	mi, ok := s.metainfo[name]
	if !ok {
		return nil, ErrNotFound
	}
	return mi.Serialize()
}

func (s *testMetaInfoStore) SetMetaInfo(mi *torlib.MetaInfo) error {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.metainfo[mi.Name()]; ok {
		return ErrExists
	}
	s.metainfo[mi.Name()] = mi
	return nil
}
