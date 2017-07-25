package service

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"

	"code.uber.internal/go-common.git/x/log"
	"github.com/jackpal/bencode-go"

	config "code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils"
)

// announceStore is a subset of the storage.Storage interface.
type announceStore interface {
	Update(p *storage.PeerInfo) error
	Read(infoHash string) ([]*storage.PeerInfo, error)
}

type announceHandler struct {
	config config.AnnouncerConfig
	store  announceStore
	policy peerhandoutpolicy.PeerHandoutPolicy
}

// AnnouncerResponse follows a bittorrent tracker protocol
// for tracker based peer discovery
type AnnouncerResponse struct {
	Interval int64              `bencode:"interval"`
	Peers    []storage.PeerInfo `bencode:"peers"`
}

func (h *announceHandler) Get(w http.ResponseWriter, r *http.Request) {
	log.Debugf("Received announce requet from: %s", r.Host)

	queryValues := r.URL.Query()

	infoHash := hex.EncodeToString([]byte(queryValues.Get("info_hash")))
	peerID := hex.EncodeToString([]byte(queryValues.Get("peer_id")))
	peerPortStr := queryValues.Get("port")
	peerIPStr := queryValues.Get("ip")
	peerDC := queryValues.Get("dc")
	peerBytesDownloadedStr := queryValues.Get("downloaded")
	peerBytesUploadedStr := queryValues.Get("uploaded")
	peerBytesLeftStr := queryValues.Get("left")
	peerEvent := queryValues.Get("event")

	peerPort, err := strconv.ParseInt(peerPortStr, 10, 64)
	if err != nil {
		log.Infof("Port is not parsable: %s", formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerIPInt32, err := strconv.ParseInt(peerIPStr, 10, 32)
	if err != nil {
		log.Infof("Peer's ip address is not a valid integer: %s", formatRequest(r))
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

	peerIP := utils.Int32toIP(int32(peerIPInt32)).String()

	peer := &storage.PeerInfo{
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

	err = h.store.Update(peer)
	if err != nil {
		log.Infof("Could not update storage for: hash %s, error: %s, request: %s",
			infoHash, err.Error(), formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerInfos, err := h.store.Read(infoHash)
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
	derefPeerInfos := make([]storage.PeerInfo, len(peerInfos))
	for i, p := range peerInfos {
		derefPeerInfos[i] = *p
	}

	// write peers bencoded
	err = bencode.Marshal(w, AnnouncerResponse{
		Interval: h.config.AnnounceInterval,
		Peers:    derefPeerInfos,
	})
	if err != nil {
		log.Infof("Bencode marshalling has failed: %s for request: %s", err.Error(), formatRequest(r))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
