package service

import (
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.

	"github.com/pressly/chi"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/handler"
)

// Handler instantiates a new handler for the tracker service.
func Handler(
	config Config,
	stats tally.Scope,
	policy peerhandoutpolicy.PeerHandoutPolicy,
	peerStore storage.PeerStore,
	metaInfoStore storage.MetaInfoStore,
	manifestStore storage.ManifestStore,
	originCluster blobclient.ClusterClient) http.Handler {

	stats = stats.SubScope("service")

	announce := &announceHandler{
		config,
		peerStore,
		policy,
		originCluster,
	}
	health := &healthHandler{}
	metainfo := newMetaInfoHandler(config.MetaInfo, metaInfoStore, originCluster)
	manifest := &manifestHandler{manifestStore}

	r := chi.NewRouter()
	announce.setRoutes(r, stats)
	health.setRoutes(r, stats)
	metainfo.setRoutes(r, stats)
	manifest.setRoutes(r, stats)

	// Serves /debug/pprof endpoints.
	r.Mount("/", http.DefaultServeMux)

	return r
}

func (h *healthHandler) setRoutes(r chi.Router, stats tally.Scope) {
	r.Group(func(r chi.Router) {
		estats := stats.SubScope("health")
		r.Use(middleware.Counter(estats))
		r.Use(middleware.ElapsedTimer(estats))

		r.Get("/health", h.Get)
	})
}

func (h *metaInfoHandler) setRoutes(r chi.Router, stats tally.Scope) {
	r.Group(func(r chi.Router) {
		estats := stats.SubScope("namespace.blobs.metainfo")
		r.Use(middleware.Counter(estats))
		r.Use(middleware.ElapsedTimer(estats))

		r.Get("/namespace/:namespace/blobs/:digest/metainfo", handler.Wrap(h.get))
	})
}

func (h *manifestHandler) setRoutes(r chi.Router, stats tally.Scope) {
	r.Group(func(r chi.Router) {
		estats := stats.SubScope("manifest")
		r.Use(middleware.Counter(estats))
		r.Use(middleware.ElapsedTimer(estats))

		r.Get("/manifest/:name", handler.Wrap(h.Get))
		r.Post("/manifest/:name", handler.Wrap(h.Post))
	})
}

func (h *announceHandler) setRoutes(r chi.Router, stats tally.Scope) {
	r.Group(func(r chi.Router) {
		estats := stats.SubScope("announce")
		r.Use(middleware.Counter(estats))
		r.Use(middleware.ElapsedTimer(estats))

		r.Get("/announce", handler.Wrap(h.Get))
	})
}
