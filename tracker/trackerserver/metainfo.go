package trackerserver

import (
	"fmt"
	"net/http"
	"net/url"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
	"github.com/pressly/chi"
)

func (s *Server) getMetaInfoHandler(w http.ResponseWriter, r *http.Request) error {
	namespace := chi.URLParam(r, "namespace")
	if namespace == "" {
		return handler.Errorf("empty namespace").Status(http.StatusBadRequest)
	}
	d, err := parseDigest(r)
	if err != nil {
		return handler.Errorf("parse digest: %s", err).Status(http.StatusBadRequest)
	}

	raw, err := s.metaInfoStore.GetMetaInfo(d.Hex())
	if err != nil {
		if err == storage.ErrNotFound {
			return s.startMetaInfoDownload(namespace, d)
		}
		return handler.Errorf("storage: %s", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(raw)
	return nil
}

func (s *Server) startMetaInfoDownload(namespace string, d core.Digest) error {
	id := namespace + ":" + d.Hex()
	err := s.metaInfoRequestCache.Start(id, func() error {

		getMetaInfoTimer := s.stats.Timer("get_metainfo").Start()
		mi, err := s.originCluster.GetMetaInfo(namespace, d)
		if err != nil {
			log.With("name", d.Hex()).Infof("Caching origin metainfo lookup error: %s", err)
			return err
		}
		getMetaInfoTimer.Stop()

		if err := s.metaInfoStore.SetMetaInfo(mi); err != nil {
			if err != storage.ErrExists {
				log.With("name", d.Hex()).Errorf("Caching metainfo storage error: %s", err)
				return fmt.Errorf("cache metainfo: %s", err)
			}
			return nil
		}
		log.With("name", d.Hex()).Info("Successfully cached metainfo")
		return nil
	})
	if err == dedup.ErrRequestPending || err == nil {
		return handler.ErrorStatus(http.StatusAccepted)
	} else if err == dedup.ErrWorkersBusy {
		return handler.ErrorStatus(http.StatusServiceUnavailable)
	} else if serr, ok := err.(httputil.StatusError); ok {
		// Propagate any errors received from origin.
		return handler.Errorf("origin: %s", serr.ResponseDump).Status(serr.Status)
	}
	return err
}

// parseDigest parses a digest from a url path parameter, e.g. "/blobs/:digest".
func parseDigest(r *http.Request) (digest core.Digest, err error) {
	d := chi.URLParam(r, "digest")
	if len(d) == 0 {
		return digest, fmt.Errorf("empty digest")
	}
	raw, err := url.PathUnescape(d)
	if err != nil {
		return digest, fmt.Errorf("path unescape: %s", err)
	}
	digest, err = core.NewDigestFromString(raw)
	if err != nil {
		return digest, fmt.Errorf("parse digest: %s", err)
	}
	return digest, nil
}
