package service

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/tracker/storage"
	"github.com/pressly/chi"
)

type manifestHandler struct {
	store storage.ManifestStore
}

func (h *manifestHandler) Get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	name := chi.URLParam(r, "name")
	if len(name) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		writeJSONErrorf(w, "Failed to parse an empty tag name")
		return
	}

	name, err := url.QueryUnescape(name)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSONErrorf(w, "cannot unescape manifest name: %s, error: %s", name, err)
		log.WithFields(log.Fields{
			"name": name, "error": err, "request": formatRequest(r),
		}).Error("Failed to unescape manifest name")
		return
	}

	manifest, err := h.store.GetManifest(name)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
		writeJSONErrorf(w, "cannot get manifest name: %s, error: %s", name, err)
		log.WithFields(log.Fields{
			"name": name, "error": err, "request": formatRequest(r),
		}).Error("cannot get manifest")
		return
	}

	io.WriteString(w, manifest)
	w.WriteHeader(http.StatusOK)
	log.Infof("Got manifest for %s", name)
}

func (h *manifestHandler) Post(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	name := chi.URLParam(r, "name")

	if len(name) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		writeJSONErrorf(w, "Failed to parse a tag name")
		return
	}

	name, err := url.QueryUnescape(name)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSONErrorf(w, "cannot unescape manifest name: %s, error: %s", name, err)
		log.WithFields(log.Fields{
			"name": name, "error": err, "request": formatRequest(r),
		}).Error("Failed to unescape manifest name")
		return
	}

	defer r.Body.Close()
	manifest, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSONErrorf(w, "Could not read manifest on post for %s and error: %s", name, err)
		log.WithFields(log.Fields{
			"name": name, "error": err, "request": formatRequest(r),
		}).Error("Failed to read manifest payload")
		return
	}

	var jsonManifest map[string]interface{}
	if err := json.Unmarshal(manifest, &jsonManifest); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeJSONErrorf(w, "Manifest is an invalid json for %s, manifest %s and error: %s",
			name, manifest[:], err)
		log.WithFields(log.Fields{
			"name": name, "manifest": manifest[:], "error": err, "request": formatRequest(r),
		}).Error("Failed to parse manifest")
		return
	}

	err = h.store.CreateManifest(name, string(manifest[:]))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSONErrorf(w, "Failed to update manifest for %s with manifest %s and error: %s",
			name, manifest, err)
		log.WithFields(log.Fields{
			"name": name, "manifest": manifest[:], "error": err, "request": formatRequest(r),
		}).Error("Failed to update manifest")
		return
	}

	w.WriteHeader(http.StatusOK)
	log.Infof("Updated manifest successfully for %s", name)
}
