package trackerserver

import (
	"errors"
	"sync"

	"github.com/pressly/chi"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/testutil"
)

type testPeerStore struct {
	sync.Mutex
	torrents map[string][]core.PeerInfo
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

// TestAnnouncer is a test utility which starts an in-memory tracker which listens
// for announce requests. Returns the "ip:port" the tracker is running on, and a
// closure for stopping the tracker.
func TestAnnouncer() (addr string, stop func()) {
	policy, err := peerhandoutpolicy.Get("ipv4netmask", "completeness")
	if err != nil {
		log.Fatalf("Failed to get peer handout policy: %s", err)
	}

	announce := &announceHandler{
		store: &testPeerStore{
			torrents: make(map[string][]core.PeerInfo),
		},
		policy: policy,
	}

	r := chi.NewRouter()
	r.Get("/announce", handler.Wrap(announce.Get))

	return testutil.StartServer(r)
}
