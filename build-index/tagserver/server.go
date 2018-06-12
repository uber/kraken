package tagserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/build-index/tagtype"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/stringset"

	"github.com/andres-erbsen/clock"
	"github.com/pressly/chi"
	chimiddleware "github.com/pressly/chi/middleware"
	"github.com/uber-go/tally"
)

// Server provides tag operations for the build-index.
type Server struct {
	config            Config
	stats             tally.Scope
	backends          *backend.Manager
	localOriginDNS    string
	localOriginClient blobclient.ClusterClient
	localReplicas     stringset.Set
	cache             *dedup.Cache

	// For async new tag replication.
	remotes               tagreplication.Remotes
	tagReplicationManager persistedretry.Manager
	provider              tagclient.Provider

	// For checking if a tag has all dependent blobs.
	tagTypes tagtype.Manager
}

// New creates a new Server.
func New(
	config Config,
	stats tally.Scope,
	backends *backend.Manager,
	localOriginDNS string,
	localOriginClient blobclient.ClusterClient,
	localReplicas stringset.Set,
	remotes tagreplication.Remotes,
	tagReplicationManager persistedretry.Manager,
	provider tagclient.Provider,
	tagTypes tagtype.Manager) *Server {

	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "tagserver",
	})

	cache := dedup.NewCache(config.Cache, clock.New(), &tagResolver{backends, remotes, provider})
	return &Server{
		config:                config,
		stats:                 stats,
		backends:              backends,
		localOriginDNS:        localOriginDNS,
		localOriginClient:     localOriginClient,
		localReplicas:         localReplicas,
		cache:                 cache,
		remotes:               remotes,
		tagReplicationManager: tagReplicationManager,
		provider:              provider,
		tagTypes:              tagTypes,
	}
}

// Handler returns an http.Handler for s.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))
	r.Put("/tags/:tag/digest/:digest", handler.Wrap(s.putTagHandler))
	r.Head("/tags/:tag", handler.Wrap(s.hasTagHandler))
	r.Get("/tags/:tag", handler.Wrap(s.getTagHandler))
	r.Post("/remotes/tags/:tag", handler.Wrap(s.replicateTagHandler))
	r.Get("/origin", handler.Wrap(s.getOriginHandler))

	r.Post(
		"/internal/duplicate/remotes/tags/:tag/digest/:digest",
		handler.Wrap(s.duplicateReplicateTagHandler))

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

	depResolver, err := s.tagTypes.GetDependencyResolver(tag)
	if err != nil {
		return handler.Errorf("get dependency resolver: %s", err)
	}

	deps, err := depResolver.Resolve(tag, d)
	if err != nil {
		return handler.Errorf("get dependencies: %s", err)
	}

	for _, dep := range deps {
		ok, err := s.localOriginClient.CheckBlob(tag, dep)
		if err != nil {
			return handler.Errorf("check blob: %s", err)
		}
		if !ok {
			return handler.Errorf("tag has missing dependency: %s", dep)
		}
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
	local, err := strconv.ParseBool(httputil.GetQueryArg(r, "local", "false"))
	if err != nil {
		return handler.Errorf("parse arg `local` as bool: %s", err)
	}
	tag, err := parseTag(r)
	if err != nil {
		return err
	}

	v, err := s.cache.Get(tagContext{local}, tag)
	if err != nil {
		return err
	}
	digest := v.(core.Digest)
	if _, err := io.WriteString(w, digest.String()); err != nil {
		return handler.Errorf("write digest: %s", err)
	}
	return nil
}

func (s *Server) hasTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := parseTag(r)
	if err != nil {
		return err
	}

	client, err := s.backends.GetClient(tag)
	if err != nil {
		return handler.Errorf("backend manager: %s", err)
	}
	if _, err := client.Stat(tag); err != nil {
		if err == backenderrors.ErrBlobNotFound {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return err
	}
	return nil
}

func (s *Server) replicateTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := parseTag(r)
	if err != nil {
		return err
	}

	depResolver, err := s.tagTypes.GetDependencyResolver(tag)
	if err != nil {
		return handler.Errorf("get dependency resolver: %s", err)
	}

	v, err := s.cache.Get(tagContext{}, tag)
	if err != nil {
		return err
	}
	d := v.(core.Digest)

	deps, err := depResolver.Resolve(tag, d)
	if err != nil {
		return handler.Errorf("get dependencies: %s", err)
	}

	destinations := s.remotes.Match(tag)
	for _, dest := range destinations {
		task := tagreplication.NewTask(tag, d, deps, dest)
		if err := s.tagReplicationManager.Add(task); err != nil {
			return handler.Errorf("add replicate task: %s", err)
		}
	}

	var delay time.Duration
	for addr := range s.localReplicas { // Loops in random order.
		delay += s.config.DuplicateReplicateStagger
		client := s.provider.Provide(addr)
		if err := client.DuplicateReplicate(tag, d, deps, delay); err != nil {
			log.Errorf("Error duplicating replicate task to %s: %s", addr, err)
		}
	}

	return nil
}

func (s *Server) duplicateReplicateTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := parseTag(r)
	if err != nil {
		return err
	}
	d, err := parseDigest(r)
	if err != nil {
		return handler.Errorf("get dependency resolver: %s", err)
	}
	var req tagclient.DuplicateReplicateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return handler.Errorf("decode body: %s", err)
	}

	destinations := s.remotes.Match(tag)

	for _, dest := range destinations {
		task := tagreplication.NewTaskWithDelay(tag, d, req.Dependencies, dest, req.Delay)
		if err := s.tagReplicationManager.Add(task); err != nil {
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
