package service

import (
	"fmt"
	"io"
	"net/http"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/tracker/storage"
)

type infohashHandler struct {
	store storage.Storage
}

func (h *infohashHandler) Get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	q := r.URL.Query()
	name := q.Get("name")

	if name == "" {
		log.Errorf("Failed to get torrent info hash, no name specified: %s", formatRequest(r))
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Failed to get torrent info hash: no torrent name specified")
		return
	}

	info, err := h.store.ReadTorrent(name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to get torrent info hash: %s", err)
		log.WithFields(log.Fields{
			"name": name, "error": err,
		}).Error("Failed to get torrent info hash")
		return
	}
	if info == nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Failed to get torrent info hash: name %s not found", name)
		log.Infof("Torrent info hash is not found: %s", formatRequest(r))
		return
	}

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, info.InfoHash)
	log.Infof("Successfully got info hash for %s: %s", name, info.InfoHash)
}

func (h *infohashHandler) Post(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	q := r.URL.Query()
	name := q.Get("name")
	infoHash := q.Get("info_hash")

	if name == "" || infoHash == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Failed to create torrent: incomplete query")
		return
	}

	err := h.store.CreateTorrent(&storage.TorrentInfo{
		TorrentName: name,
		InfoHash:    infoHash,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to create torrent: %s", err)
		log.WithFields(log.Fields{
			"name": name, "error": err,
		}).Error("Failed to get torrent info hash")
		return
	}

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "Created")
}
