package tagserver

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/handler"

	"github.com/andres-erbsen/clock"
	"github.com/pressly/chi"
	chimiddleware "github.com/pressly/chi/middleware"
	"github.com/uber-go/tally"
)

// Server provides tag operations for the build-index.
type Server struct {
	config   Config
	stats    tally.Scope
	backends *backend.Manager
	cache    *dedup.Cache
}

// New creates a new Server.
func New(config Config, stats tally.Scope, backends *backend.Manager) *Server {
	stats = stats.Tagged(map[string]string{
		"module": "tagserver",
	})
	cache := dedup.NewCache(config.Cache, clock.New(), &tagResolver{backends})
	return &Server{config, stats, backends, cache}
}

// Handler returns an http.Handler for s.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))
	r.Put("/tags/:tag/digest/:digest", handler.Wrap(s.putTagHandler))
	r.Get("/tags/:tag", handler.Wrap(s.getTagHandler))

	r.Mount("/debug", chimiddleware.Profiler())

	return r
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	fmt.Fprintln(w, "OK")
	return nil
}

func (s *Server) putTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := url.PathUnescape(chi.URLParam(r, "tag"))
	if err != nil {
		return handler.Errorf("path unescape tag: %s", err).Status(http.StatusBadRequest)
	}
	d := chi.URLParam(r, "digest")
	if err := core.CheckSHA256Digest(d); err != nil {
		return handler.Errorf("invalid sha256 digest: %s", err).Status(http.StatusBadRequest)
	}
	client, err := s.backends.GetClient(tag)
	if err != nil {
		return handler.Errorf("backend manager: %s", err)
	}
	if err := client.Upload(tag, bytes.NewBufferString(d)); err != nil {
		return handler.Errorf("backend client: %s", err)
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) getTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := url.PathUnescape(chi.URLParam(r, "tag"))
	if err != nil {
		return handler.Errorf("path unescape tag: %s", err).Status(http.StatusBadRequest)
	}
	digest, err := s.cache.Get(tag)
	if err != nil {
		return err
	}
	io.WriteString(w, digest)
	return nil
}
