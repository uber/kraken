package service

import (
	"errors"
	"net/http"

	"code.uber.internal/go-common.git/x/log"
	"github.com/pressly/chi"

	config "code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"
)

type testAnnounceStore struct {
	torrents map[string][]*storage.PeerInfo
}

func (s *testAnnounceStore) Update(p *storage.PeerInfo) error {
	peers, ok := s.torrents[p.InfoHash]
	if !ok {
		s.torrents[p.InfoHash] = []*storage.PeerInfo{p}
		return nil
	}
	for i := range peers {
		if p.PeerID == peers[i].PeerID {
			peers[i] = p
			return nil
		}
	}
	s.torrents[p.InfoHash] = append(peers, p)
	return nil
}

func (s *testAnnounceStore) Read(infoHash string) ([]*storage.PeerInfo, error) {
	peers, ok := s.torrents[infoHash]
	if !ok {
		return nil, errors.New("no peers found for info hash")
	}
	return peers, nil
}

// TestAnnouncer is a test utility which starts an in-memory tracker which listens
// for announce requests on the given addr. Returns a closure for stopping the tracker.
func TestAnnouncer(addr string) func() {
	policy, ok := peerhandoutpolicy.Get("ipv4netmask", "completeness")
	if !ok {
		log.Fatal("Failed to lookup peer handout policy")
	}

	announce := &announceHandler{
		config: config.AnnouncerConfig{
			AnnounceInterval: 1,
		},
		store: &testAnnounceStore{
			torrents: make(map[string][]*storage.PeerInfo),
		},
		policy: policy,
	}

	r := chi.NewRouter()
	r.Get("/announce", announce.Get)

	server := &http.Server{
		Addr:    addr,
		Handler: r,
	}
	go server.ListenAndServe()

	return func() { server.Close() }
}
