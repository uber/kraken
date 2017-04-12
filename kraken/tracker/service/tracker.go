package service

import (
	"fmt"
	"net"
	"net/http"
	"strconv"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/kraken/tracker/storage"
	bencode "github.com/jackpal/bencode-go"
	"github.com/uber-common/bark"
)

// WebApp defines a web-app that is backed by a cache.Cache
type webApp interface {
	HealthHandler(http.ResponseWriter, *http.Request)
	GetAnnounceHandler(http.ResponseWriter, *http.Request)
	GetInfoHashHandler(http.ResponseWriter, *http.Request)
	PostInfoHashHandler(w http.ResponseWriter, r *http.Request)
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
		log.Infof("Downloaded is not parsable: %s", peerBytesDownloadedStr)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerBytesUploaded, err := strconv.ParseInt(peerBytesUploadedStr, 10, 64)
	if err != nil {
		log.Infof("Uploaded is not parsable: %s", peerBytesUploadedStr)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerBytesLeft, err := strconv.ParseInt(peerBytesLeftStr, 10, 64)
	if err != nil {
		log.Infof("Left is not parsable: %s", peerBytesLeftStr)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peerInfos, err := webApp.datastore.Read(infoHash)
	if err != nil {
		log.Infof("Could not read storage: hash %s, error: %s", infoHash, err.Error())
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
		log.Infof("Could not update storage for: hash %s, error: %s", infoHash, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// write peers bencoded
	err = bencode.Marshal(w, AnnouncerResponse{
		Interval: webApp.appCfg.Announcer.AnnounceInterval,
		Peers:    peerInfos,
	})
	if err != nil {
		log.Infof("Bencode marshalling has failed: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (webApp *webAppStruct) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("OK ;-)\n"))
}

func (webApp *webAppStruct) GetInfoHashHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	queryValues := r.URL.Query()

	name := queryValues.Get("name")
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Failed to get torrent info hash: no torrent name specified"))
		return
	}

	info, err := webApp.datastore.ReadTorrent(name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Failed to get torrent info hash: %s", err.Error())))
		log.WithFields(bark.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to get torrent info hash")
		return
	}

	if info == nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(fmt.Sprintf("Failed to get torrent info hash: name %s not found", name)))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(info.InfoHash))
}

func (webApp *webAppStruct) PostInfoHashHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	queryValues := r.URL.Query()

	name := queryValues.Get("name")
	infohash := queryValues.Get("info_hash")
	if name == "" || infohash == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Failed to create torrent: incomplete query"))
		return
	}

	err := webApp.datastore.CreateTorrent(
		&storage.TorrentInfo{
			TorrentName: name,
			InfoHash:    infohash,
		},
	)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Failed to create torrent: %s", err.Error())))
		log.WithFields(bark.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to get torrent info hash")
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Created"))
}
