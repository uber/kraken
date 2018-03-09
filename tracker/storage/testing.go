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
	torrents map[string][]core.PeerInfo
}

// TestPeerStore returns a thread-safe, in-memory peer store for testing purposes.
func TestPeerStore() PeerStore {
	return &testPeerStore{
		torrents: make(map[string][]core.PeerInfo),
	}
}

func (s *testPeerStore) UpdatePeer(p *core.PeerInfo) error {
	s.Lock()
	defer s.Unlock()

	peers, ok := s.torrents[p.InfoHash]
	if !ok {
		s.torrents[p.InfoHash] = []core.PeerInfo{*p}
		return nil
	}
	for i := range peers {
		if p.PeerID == peers[i].PeerID {
			peers[i] = *p
			return nil
		}
	}
	s.torrents[p.InfoHash] = append(peers, *p)
	return nil
}

func (s *testPeerStore) GetPeers(infoHash string, n int) ([]*core.PeerInfo, error) {
	s.Lock()
	defer s.Unlock()

	peers, ok := s.torrents[infoHash]
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

func (s *testPeerStore) GetOrigins(string) ([]*core.PeerInfo, error) {
	return nil, nil
}

func (s *testPeerStore) UpdateOrigins(string, []*core.PeerInfo) error {
	return nil
}
