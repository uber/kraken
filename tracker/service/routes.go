package service

import (
	"log"
	"net/http"

	"code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"

	"github.com/pressly/chi"
)

// InitializeAPI instantiates a new web-app for the tracker
func InitializeAPI(
	cfg config.AppConfig,
	peerStore storage.PeerStore,
	torrentStore storage.TorrentStore,
	manifestStore storage.ManifestStore,
) http.Handler {

	policy, ok := peerhandoutpolicy.Get(
		cfg.PeerHandoutPolicy.Priority, cfg.PeerHandoutPolicy.Sampling)
	if !ok {
		log.Fatalf(
			"Peer handout policy not found: priority=%s sampling=%s",
			cfg.PeerHandoutPolicy.Priority, cfg.PeerHandoutPolicy.Sampling)
	}
	announce := &announceHandler{
		config: cfg.Announcer,
		store:  peerStore,
		policy: policy,
	}
	health := &healthHandler{}
	infohash := &metainfoHandler{torrentStore}
	manifest := &manifestHandler{manifestStore}

	r := chi.NewRouter()
	r.Get("/health", health.Get)
	r.Get("/announce", announce.Get)
	r.Get("/info", infohash.Get)
	r.Post("/info", infohash.Post)
	r.Get("/manifest/:name", manifest.Get)
	r.Post("/manifest/:name", manifest.Post)

	return r
}
