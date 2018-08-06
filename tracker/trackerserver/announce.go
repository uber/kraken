package trackerserver

import (
	"encoding/json"
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
)

func (s *Server) announceHandlerV1(w http.ResponseWriter, r *http.Request) error {
	req := new(announceclient.Request)
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		return handler.Errorf("json decode request: %s", err)
	}
	resp, err := s.announce(req.Name, req.InfoHash, req.Peer)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return handler.Errorf("json encode response: %s", err)
	}
	return nil
}

func (s *Server) announceHandlerV2(w http.ResponseWriter, r *http.Request) error {
	infohash, err := httputil.ParseParam(r, "infohash")
	if err != nil {
		return err
	}
	h, err := core.NewInfoHashFromHex(infohash)
	if err != nil {
		return fmt.Errorf("parse infohash: %s", err)
	}
	req := new(announceclient.Request)
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		return handler.Errorf("json decode request: %s", err)
	}
	resp, err := s.announce(req.Name, h, req.Peer)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return handler.Errorf("json encode response: %s", err)
	}
	return nil
}

func (s *Server) announce(
	name string, h core.InfoHash, peer *core.PeerInfo) (*announceclient.Response, error) {

	d, err := core.NewSHA256DigestFromHex(name)
	if err != nil {
		return nil, fmt.Errorf("parse digest: %s", err)
	}
	if err := s.peerStore.UpdatePeer(h, peer); err != nil {
		log.With(
			"hash", h,
			"peer_id", peer.PeerID).Errorf("Error updating peer: %s", err)
	}
	peers, err := s.getPeerHandout(d, h, peer)
	if err != nil {
		return nil, err
	}
	return &announceclient.Response{
		Peers:    peers,
		Interval: s.config.AnnounceInterval,
	}, nil
}

func (s *Server) getPeerHandout(
	d core.Digest, h core.InfoHash, peer *core.PeerInfo) ([]*core.PeerInfo, error) {

	if peer.Complete {
		// If the peer is announcing as complete, don't return a peer handout since
		// the peer does not need it.
		return nil, nil
	}
	var errs []error
	peers, err := s.peerStore.GetPeers(h, s.config.PeerHandoutLimit)
	if err != nil {
		errs = append(errs, fmt.Errorf("peer store: %s", err))
	}
	origins, err := s.originStore.GetOrigins(d)
	if err != nil {
		errs = append(errs, fmt.Errorf("origin store: %s", err))
	}
	peers = append(peers, origins...)
	if len(peers) == 0 {
		return nil, handler.Errorf("no peers available: %s", errutil.Join(errs))
	}
	return s.policy.SortPeers(peer, peers), nil
}
