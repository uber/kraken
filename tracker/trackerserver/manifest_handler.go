package trackerserver

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"

	"github.com/pressly/chi"

	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
)

type manifestHandler struct {
	store storage.ManifestStore
}

func (h *manifestHandler) Get(w http.ResponseWriter, r *http.Request) error {
	name, err := parseName(r)
	if err != nil {
		return err
	}

	manifest, err := h.store.GetManifest(name)
	if err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("get manifest: %s", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(manifest))
	return nil
}

func (h *manifestHandler) Post(w http.ResponseWriter, r *http.Request) error {
	name, err := parseName(r)
	if err != nil {
		return err
	}

	defer r.Body.Close()
	manifest, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return handler.Errorf("read body: %s", err)
	}

	var jsonManifest map[string]interface{}
	if err := json.Unmarshal(manifest, &jsonManifest); err != nil {
		return handler.Errorf("json unmarshal: %s", err).Status(http.StatusBadRequest)
	}

	if err := h.store.CreateManifest(name, string(manifest[:])); err != nil {
		return handler.Errorf("create manifest: %s", err)
	}

	log.Infof("Updated manifest successfully for %s", name)
	w.WriteHeader(http.StatusOK)
	return nil
}

func parseName(r *http.Request) (string, error) {
	name := chi.URLParam(r, "name")
	if len(name) == 0 {
		return "", handler.Errorf("empty name").Status(http.StatusBadRequest)
	}
	name, err := url.PathUnescape(name)
	if err != nil {
		return "", handler.Errorf("path unescape name: %s", err).Status(http.StatusBadRequest)
	}
	return name, nil
}
