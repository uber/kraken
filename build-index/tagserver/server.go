package tagserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/handler"

	"github.com/andres-erbsen/clock"
	"github.com/pressly/chi"
	chimiddleware "github.com/pressly/chi/middleware"
	"github.com/uber-go/tally"
)

// Server provides tag operations for the build-index.
type Server struct {
	config         Config
	stats          tally.Scope
	backends       *backend.Manager
	localOriginDNS string
	cache          *dedup.Cache

	// For async new tag replication.
	remotes               tagreplication.Remotes
	tagReplicationManager persistedretry.Manager
}

// New creates a new Server.
func New(
	config Config,
	stats tally.Scope,
	backends *backend.Manager,
	localOriginDNS string,
	remotes tagreplication.Remotes,
	tagReplicationManager persistedretry.Manager) *Server {

	stats = stats.Tagged(map[string]string{
		"module": "tagserver",
	})

	cache := dedup.NewCache(config.Cache, clock.New(), &tagResolver{backends})
	return &Server{config, stats, backends, localOriginDNS, cache, remotes, tagReplicationManager}
}

// Handler returns an http.Handler for s.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))
	r.Put("/tags/:tag/digest/:digest", handler.Wrap(s.putTagHandler))
	r.Get("/tags/:tag", handler.Wrap(s.getTagHandler))
	r.Post("/remotes/tags/:tag/digest/:digest", handler.Wrap(s.replicateTagHandler))
	r.Get("/origin", handler.Wrap(s.getOriginHandler))

	r.Mount("/debug", chimiddleware.Profiler())

	return r
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	fmt.Fprintln(w, "OK")
	return nil
}

func (s *Server) putTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := parseTag(r)
	if err != nil {
		return err
	}
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	client, err := s.backends.GetClient(tag)
	if err != nil {
		return handler.Errorf("backend manager: %s", err)
	}
	if err := client.Upload(tag, bytes.NewBufferString(d.String())); err != nil {
		return handler.Errorf("backend client: %s", err)
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) getTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := parseTag(r)
	if err != nil {
		return err
	}
	v, err := s.cache.Get(tag)
	if err != nil {
		return err
	}
	digest := v.(core.Digest)
	if _, err := io.WriteString(w, digest.String()); err != nil {
		return handler.Errorf("write digest: %s", err)
	}
	return nil
}

func (s *Server) replicateTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := parseTag(r)
	if err != nil {
		return err
	}
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	var req tagclient.ReplicateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return handler.Errorf("decode body: %s", err)
	}

	destinations := s.remotes.Match(tag)

	for _, dest := range destinations {
		err := s.tagReplicationManager.Add(tagreplication.NewTask(tag, d, req.Dependencies, dest))
		if err != nil {
			return handler.Errorf("add replicate task: %s", err)
		}
	}

	return nil
}

func (s *Server) getOriginHandler(w http.ResponseWriter, r *http.Request) error {
	if _, err := io.WriteString(w, s.localOriginDNS); err != nil {
		return handler.Errorf("write local origin dns: %s", err)
	}
	return nil
}
