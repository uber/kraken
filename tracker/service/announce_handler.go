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
				})
			}
			mu.Unlock()
		}(client)
	}
	wg.Wait()

	return origins, errutil.Join(errs)
}

func (h *announceHandler) Get(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	log.Debugf("Get /announce %s", q)

	name := q.Get("name")
	infoHash := q.Get("info_hash")
	peerID := q.Get("peer_id")
	peerPortStr := q.Get("port")
	peerIP := q.Get("ip")
	peerDC := q.Get("dc")
	peerBytesDownloadedStr := q.Get("downloaded")
	peerBytesUploadedStr := q.Get("uploaded")
	peerBytesLeftStr := q.Get("left")
	peerEvent := q.Get("event")

	peerPort, err := strconv.ParseInt(peerPortStr, 10, 64)
	if err != nil {
		log.Infof("Port is not parsable: %s", formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerBytesDownloaded, err := strconv.ParseInt(peerBytesDownloadedStr, 10, 64)
	if err != nil {
		log.Infof("Downloaded is not parsable: %s", formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerBytesUploaded, err := strconv.ParseInt(peerBytesUploadedStr, 10, 64)
	if err != nil {
		log.Infof("Uploaded is not parsable: %s", formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerBytesLeft, err := strconv.ParseUint(peerBytesLeftStr, 10, 64)
	if err != nil {
		log.Infof("left is not parsable: %s", formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peer := &torlib.PeerInfo{
		InfoHash:        infoHash,
		PeerID:          peerID,
		IP:              peerIP,
		Port:            peerPort,
		DC:              peerDC,
		BytesUploaded:   peerBytesUploaded,
		BytesDownloaded: peerBytesDownloaded,
		// TODO (@evelynl): our torrent library use uint64 as bytes left but database/sql does not support it
		BytesLeft: int64(peerBytesLeft),
		Event:     peerEvent,
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
			http.Error(w, fmt.Sprintf("error getting peers: %s", err), http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}

	err = h.policy.AssignPeerPriority(peer, peers)
	if err != nil {
		log.Infof("Could not apply a peer handout priority policy: %s, error : %s, request: %s",
			infoHash, err.Error(), formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// TODO(codyg): Accept peer limit query argument.
	peers, err = h.policy.SamplePeers(peers, len(peers))
	if err != nil {
		msg := "Could not apply peer handout sampling policy"
		log.With("error", err.Error(), "info_hash", infoHash, "request", formatRequest(r)).Info(msg)
		http.Error(w, fmt.Sprintf("%s: %v", msg, err), http.StatusInternalServerError)
		return
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
		log.Infof("Bencode marshalling has failed: %s for request: %s", err.Error(), formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
