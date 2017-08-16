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
	appCfg config.AppConfig,
	store storage.Storage,
) http.Handler {

	policy, ok := peerhandoutpolicy.Get(
		appCfg.PeerHandoutPolicy.Priority, appCfg.PeerHandoutPolicy.Sampling)
	if !ok {
		log.Fatalf(
			"Peer handout policy not found: priority=%s sampling=%s",
			appCfg.PeerHandoutPolicy.Priority, appCfg.PeerHandoutPolicy.Sampling)
	}
	announce := &announceHandler{
		config: appCfg.Announcer,
		store:  store,
		policy: policy,
	}
	health := &healthHandler{}
	infohash := &metainfoHandler{store}
	manifest := &manifestHandler{store}

	r := chi.NewRouter()
	r.Get("/health", health.Get)
	r.Get("/announce", announce.Get)
	r.Get("/info", infohash.Get)
	r.Post("/info", infohash.Post)
	r.Get("/manifest/:name", manifest.Get)
	r.Post("/manifest/:name", manifest.Post)

	return r
}
