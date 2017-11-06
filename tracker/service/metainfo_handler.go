package service

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/storage"
)

type metainfoHandler struct {
	store storage.TorrentStore
}

func (h *metainfoHandler) Get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	q := r.URL.Query()
	name := q.Get("name")

	if name == "" {
		http.Error(w, "no name specified", http.StatusBadRequest)
		return
	}

	metaRaw, err := h.store.GetTorrent(name)
	if err != nil {
		if err == storage.ErrNotFound {
			http.Error(w, "torrent not found", http.StatusNotFound)
		} else {
			http.Error(w, fmt.Sprintf("storage: %s", err), http.StatusInternalServerError)
		}
		log.WithFields(log.Fields{"name": name}).Errorf("Error getting torrent metainfo: %s", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, metaRaw)
	log.Infof("Successfully got metainfo for %s", name)
}

func (h *metainfoHandler) Post(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	metaRaw, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("read body: %s", err), http.StatusInternalServerError)
		log.Errorf("Error reading request body: %s", err)
		return
	}

	mi, err := torlib.DeserializeMetaInfo(metaRaw)
	if err != nil {
		http.Error(w, fmt.Sprintf("deserialize metainfo: %s", err), http.StatusBadRequest)
		log.Errorf("Error deserializing metainfo body: %s", err)
		return
	}

	if err := h.store.CreateTorrent(mi); err != nil {
		if err == storage.ErrExists {
			http.Error(w, fmt.Sprintf("metainfo already exists for name %s", mi.Name()), http.StatusConflict)
		} else {
			http.Error(w, fmt.Sprintf("storage: %s", err), http.StatusInternalServerError)
		}
		log.WithFields(log.Fields{"name": mi.Name()}).Errorf("Failed to create torrent: %s", err)
		return
	}

	log.WithFields(log.Fields{"name": mi.Name()}).Info("Wrote torrent metainfo")
	w.WriteHeader(http.StatusOK)
}
