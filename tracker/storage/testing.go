package storage

import (
	"errors"
	"sync"

	"code.uber.internal/infra/kraken/core"
)

type testMetaInfoStore struct {
	sync.Mutex
	metainfo map[string]*core.MetaInfo
}

// TestMetaInfoStore returns a thread-safe, in-memory metainfo store for testing
// purposes.
func TestMetaInfoStore() MetaInfoStore {
	return &testMetaInfoStore{
		metainfo: make(map[string]*core.MetaInfo),
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

func (s *testMetaInfoStore) SetMetaInfo(mi *core.MetaInfo) error {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.metainfo[mi.Name()]; ok {
		return ErrExists
	}
	s.metainfo[mi.Name()] = mi
	return nil
}

type testPeerStore struct {
	sync.Mutex
	torrents map[core.InfoHash][]core.PeerInfo
}

// TestPeerStore returns a thread-safe, in-memory peer store for testing purposes.
func TestPeerStore() PeerStore {
	return &testPeerStore{
		torrents: make(map[core.InfoHash][]core.PeerInfo),
	}
}

func (s *testPeerStore) UpdatePeer(h core.InfoHash, p *core.PeerInfo) error {
	s.Lock()
	defer s.Unlock()

	peers, ok := s.torrents[h]
	if !ok {
		s.torrents[h] = []core.PeerInfo{*p}
		return nil
	}
	for i := range peers {
		if p.PeerID == peers[i].PeerID {
			peers[i] = *p
			return nil
		}
	}
	s.torrents[h] = append(peers, *p)
	return nil
}

func (s *testPeerStore) GetPeers(h core.InfoHash, n int) ([]*core.PeerInfo, error) {
	s.Lock()
	defer s.Unlock()

	peers, ok := s.torrents[h]
	if !ok {
		return nil, errors.New("no peers found for info hash")
	}
	copies := make([]*core.PeerInfo, len(peers))
	for i, p := range peers {
		copies[i] = new(core.PeerInfo)
		*copies[i] = p
	}
	return copies, nil
}

func (s *testPeerStore) GetOrigins(core.InfoHash) ([]*core.PeerInfo, error) {
	return nil, nil
}

func (s *testPeerStore) UpdateOrigins(core.InfoHash, []*core.PeerInfo) error {
	return nil
}
