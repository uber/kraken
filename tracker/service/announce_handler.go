package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
)

type announceHandler struct {
	config        Config
	store         storage.PeerStore
	policy        peerhandoutpolicy.PeerHandoutPolicy
	originCluster blobclient.ClusterClient
}

func (h *announceHandler) fetchOrigins(infoHash, name string) ([]*torlib.PeerInfo, error) {
	var origins []*torlib.PeerInfo
	pctxs, err := h.originCluster.Owners(image.NewSHA256DigestFromHex(name))
	if err != nil {
		return nil, err
	}
	for _, pctx := range pctxs {
		origins = append(origins, &torlib.PeerInfo{
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

func (h *announceHandler) Get(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	log.Debugf("Get /announce %s", q)

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

	peer := &torlib.PeerInfo{
		InfoHash: infoHash,
		PeerID:   peerID,
		IP:       peerIP,
		Port:     peerPort,
		DC:       peerDC,
		Complete: peerComplete,
	}

	if err := h.store.UpdatePeer(peer); err != nil {
		log.With("info_hash", infoHash, "peer_id", peerID).Errorf("Error updating peer: %s", err)
	}

	if peer.Complete {
		// If the peer is announcing as complete, don't return a peer handout since
		// the peer does not need it.
		if err := json.NewEncoder(w).Encode(torlib.AnnouncerResponse{}); err != nil {
			return handler.Errorf("json encode empty response: %s", err)
		}
		return nil
	}

	var errs []error
	peers, err := h.store.GetPeers(infoHash)
	if err != nil {
		errs = append(errs, fmt.Errorf("peer storage error: %s", err))
	}
	origins, err := h.store.GetOrigins(infoHash)
	if err != nil {
		tryUpdate := true
		if err != storage.ErrNoOrigins {
			errs = append(errs, fmt.Errorf("origin peer storage error: %s", err))
			tryUpdate = false
		}
		origins, err = h.fetchOrigins(infoHash, name)
		if err != nil {
			errs = append(errs, fmt.Errorf("origin lookup error: %s", err))
			tryUpdate = false
		}
		if tryUpdate {
			if err := h.store.UpdateOrigins(infoHash, origins); err != nil {
				log.With("info_hash", infoHash).Errorf("Error upserting origins: %s", err)
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
			return handler.Errorf("error getting peers: %s", err)
		}
		return handler.ErrorStatus(http.StatusNotFound)
	}

	err = h.policy.AssignPeerPriority(peer, peers)
	if err != nil {
		return handler.Errorf("assign peer priority: %s", err)
	}

	// TODO(codyg): Accept peer limit query argument.
	peers, err = h.policy.SamplePeers(peers, len(peers))
	if err != nil {
		return handler.Errorf("sample peers: %s", err)
	}

	resp := torlib.AnnouncerResponse{
		Peers: peers,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return handler.Errorf("json encode: %s", err)
	}
	return nil
}
