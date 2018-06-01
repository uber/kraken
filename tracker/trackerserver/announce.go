package trackerserver

import (
	"encoding/json"
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
)

const (
	_seederHandoutGauge    = "seeders_handed_out"
	_seederHandoutPctGauge = "seeders_handed_out_pct"
)

func (s *Server) announceHandler(w http.ResponseWriter, r *http.Request) error {
	req := new(announceclient.Request)
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		return handler.Errorf("json decode request: %s", err)
	}
	if err := s.peerStore.UpdatePeer(req.InfoHash, req.Peer); err != nil {
		log.With(
			"hash", req.InfoHash,
			"peer_id", req.Peer.PeerID).Errorf("Error updating peer: %s", err)
	}
	peers, err := s.getPeerHandout(req.Name, req.InfoHash, req.Peer)
	if err != nil {
		return err
	}
	resp := &announceclient.Response{
		Peers:    peers,
		Interval: s.config.AnnounceInterval,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return handler.Errorf("json encode response: %s", err)
	}
	return nil
}

func (s *Server) getPeerHandout(
	name string, h core.InfoHash, peer *core.PeerInfo) ([]*core.PeerInfo, error) {

	if peer.Complete {
		// If the peer is announcing as complete, don't return a peer handout since
		// the peer does not need it.
		return nil, nil
	}
	var errs []error
	peers, err := s.peerStore.GetPeers(h, s.config.PeerHandoutLimit)
	if err != nil {
		errs = append(errs, fmt.Errorf("peer storage error: %s", err))
	}
	origins, err := s.peerStore.GetOrigins(h)
	if err != nil {
		tryUpdate := true
		if err != storage.ErrNoOrigins {
			errs = append(errs, fmt.Errorf("origin peer storage error: %s", err))
			tryUpdate = false
		}
		origins, err = s.fetchOrigins(name)
		if err != nil {
			errs = append(errs, fmt.Errorf("origin lookup error: %s", err))
			tryUpdate = false
		}
		if tryUpdate {
			if err := s.peerStore.UpdateOrigins(h, origins); err != nil {
				log.With("hash", h).Errorf("Error upserting origins: %s", err)
			}
		}
	}
	err = errutil.Join(errs)
	if err != nil {
		log.Errorf("Error getting peers: %s", err)
	}
	for _, origin := range origins {
		peers = append(peers, origin)
	}
	if len(peers) == 0 {
		if err != nil {
			return nil, handler.Errorf("error getting peers: %s", err)
		}
		return nil, handler.ErrorStatus(http.StatusNotFound)
	}

	err = s.policy.AssignPeerPriority(peer, peers)
	if err != nil {
		return nil, handler.Errorf("assign peer priority: %s", err)
	}

	// TODO(codyg): Just make this sort peers since storage already samples via limit.
	peers, err = s.policy.SamplePeers(peers, len(peers))
	if err != nil {
		return nil, handler.Errorf("sample peers: %s", err)
	}

	var peerCount int
	var seederCount int
	for _, peer := range peers {
		if !peer.Origin {
			peerCount++
			if peer.Complete {
				seederCount++
			}
		}
	}

	s.stats.Gauge(_seederHandoutGauge).Update(float64(seederCount))
	s.stats.Gauge(_seederHandoutPctGauge).Update(float64(seederCount) / float64(peerCount))

	return peers, nil
}

func (s *Server) fetchOrigins(name string) ([]*core.PeerInfo, error) {
	d, err := core.NewSHA256DigestFromHex(name)
	if err != nil {
		return nil, fmt.Errorf("new digest: %s", err)
	}
	octxs, err := s.originCluster.Owners(d)
	if err != nil {
		return nil, err
	}
	var origins []*core.PeerInfo
	for _, octx := range octxs {
		origins = append(origins, core.PeerInfoFromContext(octx, true))
	}
	return origins, nil
}
