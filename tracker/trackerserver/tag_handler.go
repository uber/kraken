package trackerserver

import (
	"bytes"
	"io"
	"net/http"
	"net/url"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
	"github.com/andres-erbsen/clock"
	"github.com/pressly/chi"
)

type tagResolver struct {
	client backend.Client
}

func (r *tagResolver) Resolve(name string) (string, error) {
	log.With("tag", name).Info("Resolving tag")
	var b bytes.Buffer
	if err := r.client.Download(name, &b); err != nil {
		return "", err
	}
	return b.String(), nil
}

type tagHandler struct {
	cache *dedup.Cache
}

func newTagHandler(tags backend.Client) *tagHandler {
	return &tagHandler{
		dedup.NewCache(dedup.CacheConfig{}, clock.New(), &tagResolver{tags}),
	}
}

func (h *tagHandler) Get(w http.ResponseWriter, r *http.Request) error {
	name, err := url.PathUnescape(chi.URLParam(r, "name"))
	if err != nil {
		return handler.Errorf("unescape name: %s", err).Status(http.StatusBadRequest)
	}
	digest, err := h.cache.Get(name)
	if err != nil {
		if err == backenderrors.ErrBlobNotFound {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return err
	}
	if _, err := io.WriteString(w, digest); err != nil {
		return handler.Errorf("write digest: %s", err)
	}
	return nil
}
