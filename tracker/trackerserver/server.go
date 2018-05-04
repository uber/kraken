package trackerserver

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.

	"github.com/andres-erbsen/clock"
	"github.com/pressly/chi"
	chimiddleware "github.com/pressly/chi/middleware"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/handler"
)

// Server serves Tracker endpoints.
type Server struct {
	config Config
	stats  tally.Scope

	peerStore     storage.PeerStore
	policy        peerhandoutpolicy.PeerHandoutPolicy
	originCluster blobclient.ClusterClient

	metaInfoStore      storage.MetaInfoStore
	getMetaInfoLimiter *dedup.Limiter

	tagCache *dedup.Cache
}

// New creates a new Server.
func New(
	config Config,
	stats tally.Scope,
	policy peerhandoutpolicy.PeerHandoutPolicy,
	peerStore storage.PeerStore,
	metaInfoStore storage.MetaInfoStore,
	originCluster blobclient.ClusterClient,
	tags backend.Client) *Server {

	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "trackerserver",
	})

	getMetaInfoLimiter := dedup.NewLimiter(
		config.GetMetaInfoLimit,
		clock.New(),
		&metaInfoGetter{stats, originCluster, metaInfoStore})

	tagCache := dedup.NewCache(config.TagCache, clock.New(), &tagResolver{tags})

	return &Server{
		config:             config,
		stats:              stats,
		peerStore:          peerStore,
		policy:             policy,
		originCluster:      originCluster,
		metaInfoStore:      metaInfoStore,
		getMetaInfoLimiter: getMetaInfoLimiter,
		tagCache:           tagCache,
	}
}

// Handler an http handler for s.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))
	r.Get("/announce", handler.Wrap(s.announceHandler))
	r.Get("/namespace/:namespace/blobs/:digest/metainfo", handler.Wrap(s.getMetaInfoHandler))
	r.Get("/tag/:name", handler.Wrap(s.getTagHandler))

	r.Mount("/debug", chimiddleware.Profiler())

	return r
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	fmt.Fprintln(w, "OK")
	return nil
}
