package service

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/andres-erbsen/clock"
	"github.com/pressly/chi"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
)

// MetaInfoConfig defines metainfo handling configuration.
type MetaInfoConfig struct {
	RequestCache dedup.RequestCacheConfig `yaml:"request_cache"`
}

type metaInfoHandler struct {
	config         MetaInfoConfig
	store          storage.MetaInfoStore
	requestCache   *dedup.RequestCache
	originResolver blobclient.ClusterResolver
}

func newMetaInfoHandler(
	config MetaInfoConfig,
	store storage.MetaInfoStore,
	originResolver blobclient.ClusterResolver) *metaInfoHandler {

	rc := dedup.NewRequestCache(config.RequestCache, clock.New())
	rc.SetNotFound(httputil.IsNotFound)

	return &metaInfoHandler{config, store, rc, originResolver}
}

func (h *metaInfoHandler) get(w http.ResponseWriter, r *http.Request) error {
	namespace := chi.URLParam(r, "namespace")
	if namespace == "" {
		return handler.Errorf("empty namespace").Status(http.StatusBadRequest)
	}
	d, err := parseDigest(r)
	if err != nil {
		return handler.Errorf("parse digest: %s", err).Status(http.StatusBadRequest)
	}

	raw, err := h.store.GetMetaInfo(d.Hex())
	if err != nil {
		if err == storage.ErrNotFound {
			return h.startMetaInfoDownload(namespace, d)
		}
		return handler.Errorf("storage: %s", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(raw)
	return nil
}

func (h *metaInfoHandler) startMetaInfoDownload(namespace string, d image.Digest) error {
	id := namespace + ":" + d.Hex()
	err := h.requestCache.Start(id, func() error {
		mi, err := h.fetchOriginMetaInfo(namespace, d)
		if err != nil {
			return err
		}
		if err := h.store.SetMetaInfo(mi); err != nil && err != storage.ErrExists {
			// Don't return error here as we only want to cache origin cluster errors.
			log.With("name", d.Hex()).Errorf("Error caching metainfo: %s", err)
		}
		return nil
	})
	if err == dedup.ErrRequestPending || err == nil {
		return handler.ErrorStatus(http.StatusAccepted)
	} else if err == dedup.ErrWorkersBusy {
		return handler.ErrorStatus(http.StatusServiceUnavailable)
	} else if serr, ok := err.(httputil.StatusError); ok {
		// Propagate any errors received from origin.
		return handler.Errorf(serr.ResponseDump).Status(serr.Status)
	}
	return err
}

func (h *metaInfoHandler) fetchOriginMetaInfo(namespace string, d image.Digest) (mi *torlib.MetaInfo, err error) {
	clients, err := h.originResolver.Resolve(d)
	if err != nil {
		return nil, handler.Errorf("resolve origins: %s", err)
	}
	blobclient.Shuffle(clients)
	for _, client := range clients {
		mi, err = client.GetMetaInfo(namespace, d)
		if _, ok := err.(httputil.NetworkError); ok {
			continue
		}
		return mi, err
	}
	return nil, err
}

// parseDigest parses a digest from a url path parameter, e.g. "/blobs/:digest".
func parseDigest(r *http.Request) (digest image.Digest, err error) {
	d := chi.URLParam(r, "digest")
	if len(d) == 0 {
		return digest, fmt.Errorf("empty digest")
	}
	raw, err := url.PathUnescape(d)
	if err != nil {
		return digest, fmt.Errorf("path unescape: %s", err)
	}
	digest, err = image.NewDigestFromString(raw)
	if err != nil {
		return digest, fmt.Errorf("parse digest: %s", err)
	}
	return digest, nil
}
