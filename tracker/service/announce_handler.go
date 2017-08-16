package service

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"

	"code.uber.internal/go-common.git/x/log"
	"github.com/jackpal/bencode-go"

	config "code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
)

// announceStore is a subset of the storage.Storage interface.
type announceStore interface {
	UpdatePeer(p *torlib.PeerInfo) error
	GetPeers(infoHash string) ([]*torlib.PeerInfo, error)
}

type announceHandler struct {
	config config.AnnouncerConfig
	store  announceStore
	policy peerhandoutpolicy.PeerHandoutPolicy
}

func (h *announceHandler) Get(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	infoHash := hex.EncodeToString([]byte(q.Get("info_hash")))
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

	err = h.store.UpdatePeer(peer)
	if err != nil {
		log.Infof("Could not update storage for: hash %s, error: %s, request: %s",
			infoHash, err.Error(), formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerInfos, err := h.store.GetPeers(infoHash)
	if err != nil {
		log.Infof("Could not read storage: hash %s, error: %s, request: %s",
			infoHash, err.Error(), formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = h.policy.AssignPeerPriority(peer, peerInfos)
	if err != nil {
		log.Infof("Could not apply a peer handout priority policy: %s, error : %s, request: %s",
			infoHash, err.Error(), formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// TODO(codyg): Accept peer limit query argument.
	peerInfos, err = h.policy.SamplePeers(peerInfos, len(peerInfos))
	if err != nil {
		msg := "Could not apply peer handout sampling policy"
		log.WithFields(log.Fields{
			"error":     err.Error(),
			"info_hash": infoHash,
			"request":   formatRequest(r),
		}).Info(msg)
		http.Error(w, fmt.Sprintf("%s: %v", msg, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// TODO(codyg): bencode can't serialize pointers, so we're forced to dereference
	// every PeerInfo first.
	derefPeerInfos := make([]torlib.PeerInfo, len(peerInfos))
	for i, p := range peerInfos {
		derefPeerInfos[i] = *p
	}

	// write peers bencoded
	err = bencode.Marshal(w, torlib.AnnouncerResponse{
		Interval: h.config.AnnounceInterval,
		Peers:    derefPeerInfos,
	})
	if err != nil {
		log.Infof("Bencode marshalling has failed: %s for request: %s", err.Error(), formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
