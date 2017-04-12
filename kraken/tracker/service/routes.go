package service

import (
	"code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/kraken/tracker/storage"

	"net/http"

	"github.com/pressly/chi"
)

// InitializeAPI instantiates a new web-app for the tracker
func InitializeAPI(
	appCfg config.AppConfig,
	storage storage.Storage,
) http.Handler {

	webApp := newWebApp(appCfg, storage)
	r := chi.NewRouter()

	// /health endpoint
	r.Get("/health", webApp.HealthHandler)

	// announce endpoint
	r.Get("/announce", webApp.GetAnnounceHandler)

	// get info hash endpoint
	r.Get("/infohash", webApp.GetInfoHashHandler)

	// post info hash endpoint
	r.Post("/infohash", webApp.PostInfoHashHandler)

	return r
}
