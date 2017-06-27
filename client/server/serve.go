package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pressly/chi"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken-torrent/metainfo"
	"code.uber.internal/infra/kraken/client/torrentclient"
	"code.uber.internal/infra/kraken/configuration"
)

const downloadTimeout = 120 // sec

// AgentWebAppError implements Error interface
type AgentWebAppError struct {
	Msg string `json:"error"`
}

// NewAgentWebAppError creates a new AgentWebAppError given error msg string
func NewAgentWebAppError(msg string) error {
	return AgentWebAppError{
		Msg: msg,
	}
}

// Error implements error.Error()
func (e AgentWebAppError) Error() string {
	return fmt.Sprintf("AgnetWebAppError: %s", e.Msg)
}

// AgentWebApp is a web application that handles query to agent
type AgentWebApp struct {
	cl     *torrentclient.Client
	config *configuration.Config
}

// NewAgentWebApp creates a new agent web application
func NewAgentWebApp(config *configuration.Config, cl *torrentclient.Client) *AgentWebApp {
	return &AgentWebApp{
		cl:     cl,
		config: config,
	}
}

// Serve starts the web service
func (awa *AgentWebApp) Serve() {
	router := chi.NewRouter()
	router.Get("/", awa.health)
	router.Get("/health", awa.health)
	router.Get("/status", awa.status)
	router.Post("/open", awa.openTorrent)
	router.Post("/download", awa.downloadTorrent)
	router.Post("/delete", awa.deleteTorrent)
	listen := fmt.Sprintf("0.0.0.0:%d", awa.config.Agent.Port)
	log.Infof("Agent web app listening at %s", listen)
	log.Fatal(http.ListenAndServe(listen, router))
}

func (awa *AgentWebApp) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("OK"))
}

func (awa *AgentWebApp) status(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	// writes all torrents statuses
	awa.cl.WriteStatus(w)
}

// openTorrent adds a torrent if not existed in the torrent client yet
// and also start downloading if not on disk yet
// it is useful on the original agents upon restart
// the original agents will slowly load the torrents on disk
// this endpoint would open the torrent (if exists on disk) immediately
func (awa *AgentWebApp) openTorrent(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	q := r.URL.Query()
	ih := q.Get("info_hash")
	tr := q.Get("announce")

	// returns error if missing info hash
	if ih == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Failed to open torrent. Missing info_hash in request."))
		return
	}

	t, new, err := awa.cl.AddTorrentInfoHash(metainfo.NewHashFromHex(ih))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	log.Infof("Added torrent info hash")

	// if the torrent is new to client, add tracker
	if new {
		if ih == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Failed to open torrent. Missing announce in request."))
			return
		}

		t.AddTrackers([][]string{{tr}})
		<-t.GotInfo()
		log.Info("Got Torrent Info")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("New torrent added and is ready to be downloaded."))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Torrent already opened. No need to do anything."))
}

// downloadTorrent downloads an opened torrent
func (awa *AgentWebApp) downloadTorrent(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	q := r.URL.Query()
	ih := q.Get("info_hash")

	// returns error if missing info hash
	if ih == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Failed to download torrent. Missing info_hash in request."))
		return
	}

	// get the opened torrent from client given info hash
	t, ok, err := awa.cl.Torrent(metainfo.NewHashFromHex(ih))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Failed to download torrent. Torrent not found. Did you forget to call open?"))
		return
	}

	// timer sets a timeout for waiting on t.GoInfo() so the call will not be blocking
	timer := time.NewTimer(time.Second * downloadTimeout)
	select {
	case <-t.GotInfo():
		break
	case <-timer.C:
		log.Info("Waiting for torrent's info timed out")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to download torrent. Timeout waiting to get info."))
		return
	}

	// start downloading torrent
	log.Info("Got Torrent Info. Start downloading...")
	t.DownloadAll()
	log.Info("Torrent download completed.")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("New torrent downloaded"))
	return
}

func (awa *AgentWebApp) deleteTorrent(w http.ResponseWriter, r *http.Request) {
	// TODO (@evelynl): should we consider any authentication for tracker?
	w.Header().Set("Content-Type", "application/json")

	q := r.URL.Query()
	name := q.Get("name")
	ih := q.Get("info_hash")

	var err error
	defer func() {
		if err != nil {
			log.Error(err)
			json.NewEncoder(w).Encode(AgentWebAppError{
				Msg: err.Error(),
			})
		}
	}()

	// Returns error if missing name
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		err = NewAgentWebAppError("Failed to delete torrent. Name not specified.")
		return
	}

	if ih == "" {
		w.WriteHeader(http.StatusBadRequest)
		err = NewAgentWebAppError("Failed to delete torrent. infohash not specified.")
		return
	}

	// Delete torrent
	err = awa.cl.DeleteTorrent(name, metainfo.NewHashFromHex(ih))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		err = NewAgentWebAppError(err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{}"))
	return
}
