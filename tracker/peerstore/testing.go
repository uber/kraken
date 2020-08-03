// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package peerstore

import (
	"errors"
	"sync"

	"github.com/uber/kraken/core"
)

type testStore struct {
	sync.Mutex
	torrents map[core.InfoHash][]core.PeerInfo
}

// TestStore returns a thread-safe, in-memory peer store for testing purposes.
func NewTestStore() Store {
	return &testStore{
		torrents: make(map[core.InfoHash][]core.PeerInfo),
	}
}

func (s *testStore) Close() {}

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
