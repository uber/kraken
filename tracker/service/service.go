package service

import (
	"net/http"

	"github.com/pressly/chi"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"
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
	infohash := &metainfoHandler{torrentStore}
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

		r.Get("/announce", announce.Get)
	})

	r.Group(func(r chi.Router) {
		estats := stats.SubScope("info")
		r.Use(middleware.Counter(estats))
		r.Use(middleware.ElapsedTimer(estats))

		r.Get("/info", infohash.Get)
		r.Post("/info", infohash.Post)
	})

	r.Group(func(r chi.Router) {
		estats := stats.SubScope("manifest")
		r.Use(middleware.Counter(estats))
		r.Use(middleware.ElapsedTimer(estats))

		r.Get("/manifest/:name", manifest.Get)
		r.Post("/manifest/:name", manifest.Post)

	})

	return r
}
