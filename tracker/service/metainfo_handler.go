package service

import (
	"io/ioutil"
	"net/http"

	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
)

type metainfoHandler struct {
	store storage.TorrentStore
}

func (h *metainfoHandler) Get(w http.ResponseWriter, r *http.Request) error {
	name := r.URL.Query().Get("name")
	if name == "" {
		return handler.Errorf("empty name").Status(http.StatusBadRequest)
	}

	metaRaw, err := h.store.GetTorrent(name)
	if err != nil {
		if err == storage.ErrNotFound {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("storage: %s", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(metaRaw))
	return nil
}

func (h *metainfoHandler) Post(w http.ResponseWriter, r *http.Request) error {
	defer r.Body.Close()
	metaRaw, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return handler.Errorf("read body: %s", err)
	}

	mi, err := torlib.DeserializeMetaInfo(metaRaw)
	if err != nil {
		return handler.Errorf("deserialize metainfo: %s", err).Status(http.StatusBadRequest)
	}

	if err := h.store.CreateTorrent(mi); err != nil {
		if err == storage.ErrExists {
			return handler.ErrorStatus(http.StatusConflict)
		}
		return handler.Errorf("storage: %s", err)
	}

	log.With("name", mi.Name()).Info("Wrote torrent metainfo")
	w.WriteHeader(http.StatusOK)
	return nil
}
