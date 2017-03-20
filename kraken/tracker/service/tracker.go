package service

import (
	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/kraken/tracker/storage"

	bencode "github.com/jackpal/bencode-go"

	"net/http"
	"strconv"
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

	queryValues := r.URL.Query()

	infoHash := queryValues.Get("info_hash")
	peerID := queryValues.Get("peer_id")
	peerIP := queryValues.Get("ip")
	peerPort := queryValues.Get("port")
	peerBytesDownloadedStr := queryValues.Get("downloaded")
	peerBytesUploadedStr := queryValues.Get("uploaded")
	peerBytesLeftStr := queryValues.Get("left")
	peerEvent := queryValues.Get("event")

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

	w.Header().Set("Content-Type", "application/json")

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
