package service

import (
	"net/http"

	"github.com/pressly/chi"

	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"
)

// Handler instantiates a new handler for the tracker service.
func Handler(
	config Config,
	policy peerhandoutpolicy.PeerHandoutPolicy,
	peerStore storage.PeerStore,
	torrentStore storage.TorrentStore,
	manifestStore storage.ManifestStore,
) http.Handler {

	announce := &announceHandler{
		config: config,
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
