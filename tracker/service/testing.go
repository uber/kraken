package service

import (
	"errors"
	"sync"
	"time"

	"github.com/pressly/chi"

	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/testutil"
)

type testPeerStore struct {
	sync.Mutex
	torrents map[string][]torlib.PeerInfo
}

func (s *testPeerStore) UpdatePeer(p *torlib.PeerInfo) error {
	s.Lock()
	defer s.Unlock()

	peers, ok := s.torrents[p.InfoHash]
	if !ok {
		s.torrents[p.InfoHash] = []torlib.PeerInfo{*p}
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

func (s *testPeerStore) GetPeers(infoHash string) ([]*torlib.PeerInfo, error) {
	s.Lock()
	defer s.Unlock()

	peers, ok := s.torrents[infoHash]
	if !ok {
		return nil, errors.New("no peers found for info hash")
	}
	copies := make([]*torlib.PeerInfo, len(peers))
	for i, p := range peers {
		copies[i] = new(torlib.PeerInfo)
		*copies[i] = p
	}
	return copies, nil
}

func (s *testPeerStore) GetOrigins(string) ([]*torlib.PeerInfo, error) {
	return nil, nil
}

func (s *testPeerStore) UpdateOrigins(string, []*torlib.PeerInfo) error {
	return nil
}

// TestAnnouncer is a test utility which starts an in-memory tracker which listens
// for announce requests. Returns the "ip:port" the tracker is running on, and a
// closure for stopping the tracker.
func TestAnnouncer() (addr string, stop func()) {
	policy, err := peerhandoutpolicy.Get("ipv4netmask", "completeness")
	if err != nil {
		log.Fatalf("Failed to get peer handout policy: %s", err)
	}

	announce := &announceHandler{
		config: Config{
			AnnounceInterval: time.Second,
		},
		store: &testPeerStore{
			torrents: make(map[string][]torlib.PeerInfo),
		},
		policy: policy,
	}

	r := chi.NewRouter()
	r.Get("/announce", handler.Wrap(announce.Get))

	return testutil.StartServer(r)
}
