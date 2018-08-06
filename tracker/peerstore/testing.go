package peerstore

import (
	"errors"
	"sync"

	"code.uber.internal/infra/kraken/core"
)

type testStore struct {
	sync.Mutex
	torrents map[core.InfoHash][]core.PeerInfo
}

// TestStore returns a thread-safe, in-memory peer store for testing purposes.
func TestStore() Store {
	return &testStore{
		torrents: make(map[core.InfoHash][]core.PeerInfo),
	}
}

func (s *testStore) UpdatePeer(h core.InfoHash, p *core.PeerInfo) error {
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

func (s *testStore) GetPeers(h core.InfoHash, n int) ([]*core.PeerInfo, error) {
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
