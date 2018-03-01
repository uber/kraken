package trackerserver

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

// Handler instantiates a new handler for the tracker server.
func Handler(
	config Config,
	stats tally.Scope,
	policy peerhandoutpolicy.PeerHandoutPolicy,
	peerStore storage.PeerStore,
	metaInfoStore storage.MetaInfoStore,
	originCluster blobclient.ClusterClient) http.Handler {

	stats = stats.Tagged(map[string]string{
		"module": "trackerserver",
	})

	announce := &announceHandler{peerStore, policy, originCluster}
	health := &healthHandler{}
	metainfo := newMetaInfoHandler(config.MetaInfo, stats, metaInfoStore, originCluster)

	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(stats))
	r.Use(middleware.LatencyTimer(stats))

	announce.setRoutes(r)
	health.setRoutes(r)
	metainfo.setRoutes(r)

	// Serves /debug/pprof endpoints.
	r.Mount("/", http.DefaultServeMux)

	return r
}

func (h *healthHandler) setRoutes(r chi.Router) {
	r.Get("/health", h.Get)
}

func (h *metaInfoHandler) setRoutes(r chi.Router) {
	r.Get("/namespace/:namespace/blobs/:digest/metainfo", handler.Wrap(h.get))
}

func (h *announceHandler) setRoutes(r chi.Router) {
	r.Get("/announce", handler.Wrap(h.Get))
}
