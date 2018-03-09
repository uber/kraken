package trackerserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
)

func (s *Server) announceHandler(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	name := q.Get("name")
	infoHash := q.Get("info_hash")
	peerID := q.Get("peer_id")
	peerIP := q.Get("ip")
	peerDC := q.Get("dc")
	peerPort, err := strconv.ParseInt(q.Get("port"), 10, 64)
	if err != nil {
		return handler.Errorf("parse port: %s", err).Status(http.StatusBadRequest)
	}
	peerComplete, err := strconv.ParseBool(q.Get("complete"))
	if err != nil {
		return handler.Errorf("parse complete: %s", err).Status(http.StatusBadRequest)
	}
	peer := &core.PeerInfo{
		InfoHash: infoHash,
		PeerID:   peerID,
		IP:       peerIP,
		Port:     peerPort,
		DC:       peerDC,
		Complete: peerComplete,
	}

	if err := s.peerStore.UpdatePeer(peer); err != nil {
		log.With("info_hash", infoHash, "peer_id", peerID).Errorf("Error updating peer: %s", err)
	}

	// If the peer is announcing as complete, don't return a peer handout since
	// the peer does not need it.
	var peers []*core.PeerInfo
	if !peer.Complete {
		peers, err = s.getPeerHandout(peer, name)
		if err != nil {
			return err
		}
	}

	resp := core.AnnouncerResponse{Peers: peers}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return handler.Errorf("json encode: %s", err)
	}
	return nil
}

func (s *Server) getPeerHandout(peer *core.PeerInfo, name string) ([]*core.PeerInfo, error) {
	var errs []error
	peers, err := s.peerStore.GetPeers(peer.InfoHash, s.config.PeerHandoutLimit)
	if err != nil {
		errs = append(errs, fmt.Errorf("peer storage error: %s", err))
	}
	origins, err := s.peerStore.GetOrigins(peer.InfoHash)
	if err != nil {
		tryUpdate := true
		if err != storage.ErrNoOrigins {
			errs = append(errs, fmt.Errorf("origin peer storage error: %s", err))
			tryUpdate = false
		}
		origins, err = s.fetchOrigins(peer.InfoHash, name)
		if err != nil {
			errs = append(errs, fmt.Errorf("origin lookup error: %s", err))
			tryUpdate = false
		}
		if tryUpdate {
			if err := s.peerStore.UpdateOrigins(peer.InfoHash, origins); err != nil {
				log.With("info_hash", peer.InfoHash).Errorf("Error upserting origins: %s", err)
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

	return peers, nil
}

func (s *Server) fetchOrigins(infoHash, name string) ([]*core.PeerInfo, error) {
	var origins []*core.PeerInfo
	pctxs, err := s.originCluster.Owners(core.NewSHA256DigestFromHex(name))
	if err != nil {
		return nil, err
	}
	for _, pctx := range pctxs {
		origins = append(origins, &core.PeerInfo{
			InfoHash: infoHash,
			PeerID:   pctx.PeerID.String(),
			IP:       pctx.IP,
			Port:     int64(pctx.Port),
			DC:       pctx.Zone,
			Origin:   true,
			Complete: true,
		})
	}
	return origins, nil
}
