package trackerserver

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.

	"github.com/pressly/chi"
	chimiddleware "github.com/pressly/chi/middleware"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/peerstore"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/listener"
	"code.uber.internal/infra/kraken/utils/log"
)

// Server serves Tracker endpoints.
type Server struct {
	config Config
	stats  tally.Scope

	peerStore     peerstore.Store
	policy        *peerhandoutpolicy.PriorityPolicy
	originCluster blobclient.ClusterClient

	tagCache *dedup.Cache
}

// New creates a new Server.
func New(
	config Config,
	stats tally.Scope,
	policy *peerhandoutpolicy.PriorityPolicy,
	peerStore peerstore.Store,
	originCluster blobclient.ClusterClient) *Server {

	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "trackerserver",
	})

	return &Server{
		config:        config,
		stats:         stats,
		peerStore:     peerStore,
		policy:        policy,
		originCluster: originCluster,
	}
}

// Handler an http handler for s.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))
	r.Get("/announce", handler.Wrap(s.announceHandlerV1))
	r.Get("/announce/:infohash", handler.Wrap(s.announceHandlerV2))
	r.Get("/namespace/:namespace/blobs/:digest/metainfo", handler.Wrap(s.getMetaInfoHandler))

	r.Mount("/debug", chimiddleware.Profiler())

	return r
}

// ListenAndServe is a blocking call which runs s.
func (s *Server) ListenAndServe() error {
	log.Infof("Starting tracker server on %s", s.config.Listener)
	return listener.Serve(s.config.Listener, s.Handler())
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	fmt.Fprintln(w, "OK")
	return nil
}
