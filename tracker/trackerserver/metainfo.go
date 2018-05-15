package trackerserver

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
	"github.com/pressly/chi"
	"github.com/uber-go/tally"
)

var errCheckMetaInfoStore = errors.New("result cached in metainfo store")

type getMetaInfoRequest struct {
	namespace string
	digest    core.Digest
}

type metaInfoGetter struct {
	stats         tally.Scope
	originCluster blobclient.ClusterClient
	store         storage.MetaInfoStore
}

func (g *metaInfoGetter) Run(input interface{}) interface{} {
	req := input.(getMetaInfoRequest)

	timer := g.stats.Timer("get_metainfo").Start()
	mi, err := g.originCluster.GetMetaInfo(req.namespace, req.digest)
	if err != nil {
		if serr, ok := err.(httputil.StatusError); ok {
			// Propagate errors received from origin.
			return handler.Errorf("origin: %s", serr.ResponseDump).Status(serr.Status)
		}
		return err
	}
	timer.Stop()

	if err := g.store.SetMetaInfo(mi); err != nil {
		if err == storage.ErrExists {
			return errCheckMetaInfoStore
		}
		return fmt.Errorf("storage: %s", err)
	}

	log.With("name", req.digest.Hex()).Info("Successfully cached metainfo")

	return errCheckMetaInfoStore
}

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
	if err == storage.ErrNotFound {
		err = s.getMetaInfoLimiter.Run(getMetaInfoRequest{namespace, d}).(error)
		if err == errCheckMetaInfoStore {
			raw, err = s.metaInfoStore.GetMetaInfo(d.Hex())
		}
	}
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(raw)
	return nil
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
	digest, err = core.ParseSHA256Digest(raw)
	if err != nil {
		return digest, fmt.Errorf("parse digest: %s", err)
	}
	return digest, nil
}
