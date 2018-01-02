package service

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/jackpal/bencode-go"

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
	config         Config
	store          storage.PeerStore
	policy         peerhandoutpolicy.PeerHandoutPolicy
	originResolver blobclient.ClusterResolver
}

func (h *announceHandler) requestOrigins(infoHash, name string) ([]*torlib.PeerInfo, error) {
	clients, err := h.originResolver.Resolve(image.NewSHA256DigestFromHex(name))
	if err != nil {
		return nil, err
	}

	var mu sync.Mutex
	var origins []*torlib.PeerInfo
	var errs []error

	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		go func(client blobclient.Client) {
			defer wg.Done()
			pctx, err := client.GetPeerContext()
			mu.Lock()
			if err != nil {
				errs = append(errs, err)
			} else {
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
			mu.Unlock()
		}(client)
	}
	wg.Wait()

	return origins, errutil.Join(errs)
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
		origins, err = h.requestOrigins(infoHash, name)
		if err != nil {
			errs = append(errs, fmt.Errorf("origin lookup error: %s", err))
		}
		if len(origins) > 0 && tryUpdate {
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

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// TODO(codyg): bencode can't serialize pointers, so we're forced to dereference
	// every PeerInfo first.
	derefPeerInfos := make([]torlib.PeerInfo, len(peers))
	for i, p := range peers {
		derefPeerInfos[i] = *p
	}

	// write peers bencoded
	err = bencode.Marshal(w, torlib.AnnouncerResponse{
		Interval: int64(h.config.AnnounceInterval.Seconds()),
		Peers:    derefPeerInfos,
	})
	if err != nil {
		return handler.Errorf("bencode marshal: %s", err)
	}
	return nil
}
