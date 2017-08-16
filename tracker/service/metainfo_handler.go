package service

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/storage"
)

type metainfoHandler struct {
	store storage.Storage
}

func (h *metainfoHandler) Get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	q := r.URL.Query()
	name := q.Get("name")

	if name == "" {
		log.Errorf("Failed to get torrent metainfo, no name specified: %s", formatRequest(r))
		w.WriteHeader(http.StatusBadRequest)
		writeJSONErrorf(w, "Failed to get torrent metainfo: no torrent name specified")
		return
	}

	metaRaw, err := h.store.GetTorrent(name)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		writeJSONErrorf(w, "Failed to get torrent metainfo: %s", err)
		log.WithFields(log.Fields{
			"name": name, "error": err,
		}).Error("Failed to get torrent metainfo")
		return
	}

	w.WriteHeader(http.StatusOK)
	io.WriteString(w, metaRaw)
	log.Infof("Successfully got metainfo for %s", name)
}

func (h *metainfoHandler) Post(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	q := r.URL.Query()
	name := q.Get("name")
	infoHash := q.Get("info_hash")

	if name == "" || infoHash == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSONErrorf(w, "Failed to create torrent: incomplete query")
		return
	}

	// Read metainfo
	defer r.Body.Close()
	metaRaw, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSONErrorf(w, "Could not read metainfo payload from request for %s: %s", name, err)
		log.WithFields(log.Fields{
			"name":      name,
			"info_hash": infoHash,
			"error":     err,
			"request":   formatRequest(r),
		}).Error("Failed to read metainfo payload")
		return
	}

	// Unmarshal metainfo
	mi, err := torlib.NewMetaInfoFromBytes(metaRaw)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSONErrorf(w, "Could not create metainfo from request for %s: %s", name, err)
		log.WithFields(log.Fields{
			"name":      name,
			"info_hash": infoHash,
			"error":     err,
			"request":   formatRequest(r),
		}).Error("Failed to create metainfo from payload")
		return
	}

	// Check if infohash matches
	if mi.GetInfoHash().HexString() != infoHash {
		w.WriteHeader(http.StatusBadRequest)
		writeJSONErrorf(w, "info_hash mismatch from request for %s: requested %s, actual %s", name, infoHash, mi.GetInfoHash().HexString())
		log.WithFields(log.Fields{
			"name":      name,
			"requested": infoHash,
			"actual":    mi.GetInfoHash().HexString(),
			"request":   formatRequest(r),
		}).Error("info_hash mismatch")
		return
	}

	err = h.store.CreateTorrent(mi)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSONErrorf(w, "Failed to create torrent: %s", err)
		log.WithFields(log.Fields{
			"name":  name,
			"error": err,
		}).Error("Failed to create torrent")
		return
	}

	w.WriteHeader(http.StatusOK)
}
