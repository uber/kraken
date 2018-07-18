package trackerserver

import (
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/httputil"
)

func (s *Server) getMetaInfoHandler(w http.ResponseWriter, r *http.Request) error {
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return handler.Errorf("parse digest: %s", err).Status(http.StatusBadRequest)
	}

	timer := s.stats.Timer("get_metainfo").Start()
	mi, err := s.originCluster.GetMetaInfo(namespace, d)
	if err != nil {
		if serr, ok := err.(httputil.StatusError); ok {
			// Propagate errors received from origin.
			return handler.Errorf("origin: %s", serr.ResponseDump).Status(serr.Status)
		}
		return err
	}
	timer.Stop()

	b, err := mi.Serialize()
	if err != nil {
		return fmt.Errorf("serialize metainfo: %s", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
	return nil
}
