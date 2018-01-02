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
	torrentStore storage.TorrentStore,
	manifestStore storage.ManifestStore,
	originResolver blobclient.ClusterResolver,
) http.Handler {
	stats = stats.SubScope("service")

	announce := &announceHandler{
		config,
		peerStore,
		policy,
		originResolver,
	}
	health := &healthHandler{}
	metainfo := &metainfoHandler{torrentStore}
	manifest := &manifestHandler{manifestStore}

	r := chi.NewRouter()

	r.Group(func(r chi.Router) {
		estats := stats.SubScope("health")
		r.Use(middleware.Counter(estats))
		r.Use(middleware.ElapsedTimer(estats))

		r.Get("/health", health.Get)
	})

	r.Group(func(r chi.Router) {
		estats := stats.SubScope("announce")
		r.Use(middleware.Counter(estats))
		r.Use(middleware.ElapsedTimer(estats))

		r.Get("/announce", handler.Wrap(announce.Get))
	})

	r.Group(func(r chi.Router) {
		estats := stats.SubScope("info")
		r.Use(middleware.Counter(estats))
		r.Use(middleware.ElapsedTimer(estats))

		r.Get("/info", handler.Wrap(metainfo.Get))
		r.Post("/info", handler.Wrap(metainfo.Post))
	})

	r.Group(func(r chi.Router) {
		estats := stats.SubScope("manifest")
		r.Use(middleware.Counter(estats))
		r.Use(middleware.ElapsedTimer(estats))

		r.Get("/manifest/:name", handler.Wrap(manifest.Get))
		r.Post("/manifest/:name", handler.Wrap(manifest.Post))

	})

	// Serves /debug/pprof endpoints.
	r.Mount("/", http.DefaultServeMux)

	return r
}
