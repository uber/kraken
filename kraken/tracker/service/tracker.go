package service

import (
	"net"
	"net/http"
	"strconv"

	"code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/kraken/tracker/storage"

	"code.uber.internal/go-common.git/x/log"
	bencode "github.com/jackpal/bencode-go"
)

// WebApp defines a web-app that is backed by a cache.Cache
type webApp interface {
	HealthHandler(http.ResponseWriter, *http.Request)
	GetAnnounceHandler(http.ResponseWriter, *http.Request)
}

type webAppStruct struct {
	appCfg    config.AppConfig
	datastore storage.Storage
}

// AnnouncerResponse follows a bittorrent tracker protocol
// for tracker based peer discovery
type AnnouncerResponse struct {
	Interval int64              `bencode:"interval"`
	Peers    []storage.PeerInfo `bencode:"peers"`
}

// newWebApp instantiates a web-app API backed by the input cache
func newWebApp(cfg config.AppConfig, storage storage.Storage) webApp {
	return &webAppStruct{appCfg: cfg, datastore: storage}
}

func (webApp *webAppStruct) GetAnnounceHandler(w http.ResponseWriter, r *http.Request) {
	log.Infof("Received announce requet from: %s", r.Host)

	peerIP, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		log.Infof("Failed to get requester IP: %s", r.Host)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	queryValues := r.URL.Query()

	infoHash := queryValues.Get("info_hash")
	peerID := queryValues.Get("peer_id")
	peerPortStr := queryValues.Get("port")
	peerBytesDownloadedStr := queryValues.Get("downloaded")
	peerBytesUploadedStr := queryValues.Get("uploaded")
	peerBytesLeftStr := queryValues.Get("left")
	peerEvent := queryValues.Get("event")

	peerPort, err := strconv.ParseInt(peerPortStr, 10, 64)
	if err != nil {
		log.Infof("port is not parsable: %s", peerPortStr)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerBytesDownloaded, err := strconv.ParseInt(peerBytesDownloadedStr, 10, 64)
	if err != nil {
		log.Infof("downloaded is not parsable: %s", peerBytesDownloadedStr)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerBytesUploaded, err := strconv.ParseInt(peerBytesUploadedStr, 10, 64)
	if err != nil {
		log.Infof("uploaded is not parsable: %s", peerBytesUploadedStr)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerBytesLeft, err := strconv.ParseInt(peerBytesLeftStr, 10, 64)
	if err != nil {
		log.Infof("left is not parsable: %s", peerBytesLeftStr)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerInfos, err := webApp.datastore.Read(infoHash)
	if err != nil {
		log.Infof("could not read storage: hash %s, error: %s", infoHash, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = webApp.datastore.Update(
		&storage.PeerInfo{
			InfoHash:        infoHash,
			PeerID:          peerID,
			IP:              peerIP,
			Port:            peerPort,
			BytesUploaded:   peerBytesUploaded,
			BytesDownloaded: peerBytesDownloaded,
			BytesLeft:       peerBytesLeft,
			Event:           peerEvent})

	if err != nil {
		log.Infof("could not update storage for: hash %s, error: %s", infoHash, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// write peers bencoded
	err = bencode.Marshal(w, AnnouncerResponse{
		Interval: webApp.appCfg.Announcer.AnnounceInterval.Nanoseconds() / 1e9,
		Peers:    peerInfos,
	})
	if err != nil {
		log.Infof("bencode marshalling has failed: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (webApp *webAppStruct) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("OK ;-)\n"))
}
