// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package agentserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/containerruntime"
	"github.com/uber/kraken/lib/middleware"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/tracker/announceclient"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"

	"github.com/go-chi/chi"
	"github.com/uber-go/tally"
)

// Config defines Server configuration.
type Config struct {
	// How long a successful readiness check is valid for. If 0, disable caching successful readiness.
	ReadinessCacheTTL time.Duration `yaml:"readiness_cache_ttl"`

	// Timeout for download operations
	DownloadTimeout time.Duration `yaml:"download_timeout"`

	// Timeout for container runtime operations
	ContainerRuntimeTimeout time.Duration `yaml:"container_runtime_timeout"`

	// Timeout for readiness checks
	ReadinessTimeout time.Duration `yaml:"readiness_timeout"`

	// Enable detailed request logging
	EnableRequestLogging bool `yaml:"enable_request_logging"`
}

// applyDefaults sets default values for configuration.
func (c *Config) applyDefaults() {
	if c.DownloadTimeout == 0 {
		c.DownloadTimeout = 15 * time.Minute
	}
	if c.ContainerRuntimeTimeout == 0 {
		c.ContainerRuntimeTimeout = 10 * time.Minute
	}
	if c.ReadinessTimeout == 0 {
		c.ReadinessTimeout = 30 * time.Second
	}
}

// Server defines the agent HTTP server.
type Server struct {
	config           Config
	stats            tally.Scope
	cads             *store.CADownloadStore
	sched            scheduler.ReloadableScheduler
	tags             tagclient.Client
	ac               announceclient.Client
	containerRuntime containerruntime.Factory
	lastReady        time.Time
	lastReadyMu      sync.RWMutex
}

// New creates a new Server.
func New(
	config Config,
	stats tally.Scope,
	cads *store.CADownloadStore,
	sched scheduler.ReloadableScheduler,
	tags tagclient.Client,
	ac announceclient.Client,
	containerRuntime containerruntime.Factory,
) *Server {
	config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "agentserver",
	})

	return &Server{
		config:           config,
		stats:            stats,
		cads:             cads,
		sched:            sched,
		tags:             tags,
		ac:               ac,
		containerRuntime: containerRuntime,
	}
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	if s.config.EnableRequestLogging {
		r.Use(s.requestLoggingMiddleware)
	}

	r.Get("/health", handler.Wrap(s.healthHandler))
	r.Get("/readiness", handler.Wrap(s.readinessCheckHandler))

	r.Get("/tags/{tag}", handler.Wrap(s.getTagHandler))

	r.Get("/namespace/{namespace}/blobs/{digest}", handler.Wrap(s.downloadBlobHandler))

	r.Delete("/blobs/{digest}", handler.Wrap(s.deleteBlobHandler))

	// Preheat/preload endpoints.
	r.Get("/preload/tags/{tag}", handler.Wrap(s.preloadTagHandler))

	// Dangerous endpoint for running experiments.
	r.Patch("/x/config/scheduler", handler.Wrap(s.patchSchedulerConfigHandler))

	r.Get("/x/blacklist", handler.Wrap(s.getBlacklistHandler))

	// Serves /debug/pprof endpoints.
	r.Mount("/", http.DefaultServeMux)

	return r
}

// requestLoggingMiddleware logs request details for debugging.
func (s *Server) requestLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a logger with request context
		logger := log.With(
			"method", r.Method,
			"path", r.URL.Path,
			"remote_addr", r.RemoteAddr,
			"user_agent", r.UserAgent(),
		)

		// Add logger to context
		ctx := context.WithValue(r.Context(), "logger", logger)
		r = r.WithContext(ctx)

		next.ServeHTTP(w, r)
	})
}

// getLogger extracts logger from context, fallback to default.
func (s *Server) getLogger(ctx context.Context) *zap.SugaredLogger {
	if logger, ok := ctx.Value("logger").(*zap.SugaredLogger); ok {
		return logger
	}
	return log.With("component", "agentserver")
}

// getTagHandler proxies get tag requests to the build-index.
func (s *Server) getTagHandler(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	logger := s.getLogger(ctx)

	tag, err := httputil.ParseParam(r, "tag")
	if err != nil {
		return handler.Errorf("parse tag param: %s", err).Status(http.StatusBadRequest)
	}

	// Validate tag format
	if strings.TrimSpace(tag) == "" {
		return handler.ErrorStatus(http.StatusBadRequest)
	}

	logger.Debugw("getting tag", "tag", tag)

	d, err := s.tags.Get(tag)
	if err != nil {
		if err == tagclient.ErrTagNotFound {
			logger.Debugw("tag not found", "tag", tag)
			return handler.ErrorStatus(http.StatusNotFound)
		}
		logger.Errorw("failed to get tag", "tag", tag, "error", err)
		return handler.Errorf("get tag: %s", err)
	}

	logger.Debugw("tag found", "tag", tag, "digest", d.String())

	io.WriteString(w, d.String())
	return nil
}

// downloadBlobHandler downloads a blob through p2p.
func (s *Server) downloadBlobHandler(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	logger := s.getLogger(ctx)

	// Create timeout context for download operation
	downloadCtx, cancel := context.WithTimeout(ctx, s.config.DownloadTimeout)
	defer cancel()

	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return handler.Errorf("parse namespace param: %s", err).Status(http.StatusBadRequest)
	}

	d, err := parseDigest(r)
	if err != nil {
		return err // parseDigest already formats the error properly
	}

	// Validate inputs
	if strings.TrimSpace(namespace) == "" {
		return handler.ErrorStatus(http.StatusBadRequest)
	}

	logger = logger.With("namespace", namespace, "digest", d.String())
	logger.Debugw("downloading blob")

	// Try to get file from cache first
	reader, err := s.getCachedBlob(d)
	if err == nil {
		defer func() {
			if closeErr := reader.Close(); closeErr != nil {
				logger.Errorw("failed to close cached reader", "error", closeErr)
			}
		}()

		logger.Debugw("serving blob from cache")

		if _, err := io.Copy(w, reader); err != nil {
			return handler.Errorf("copy cached file: %s", err)
		}
		return nil
	}

	// Cache miss or error - need to download
	if err := s.downloadBlob(downloadCtx, logger, namespace, d); err != nil {
		return err
	}

	// Get the downloaded file
	reader, err = s.getCachedBlob(d)
	if err != nil {
		logger.Errorw("failed to get downloaded blob", "error", err)
		return handler.Errorf("get downloaded blob: %s", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			logger.Errorw("failed to close downloaded reader", "error", closeErr)
		}
	}()

	logger.Debugw("serving downloaded blob")

	if _, err := io.Copy(w, reader); err != nil {
		return handler.Errorf("copy downloaded file: %s", err)
	}

	return nil
}

// getCachedBlob attempts to get a blob from the cache.
func (s *Server) getCachedBlob(d core.Digest) (io.ReadCloser, error) {
	reader, err := s.cads.Cache().GetFileReader(d.Hex())
	if err != nil {
		if os.IsNotExist(err) || s.cads.InDownloadError(err) {
			return nil, fmt.Errorf("blob not in cache: %w", err)
		}
		return nil, fmt.Errorf("cache error: %w", err)
	}
	return reader, nil
}

// downloadBlob downloads a blob via the scheduler.
func (s *Server) downloadBlob(ctx context.Context, logger *zap.SugaredLogger, namespace string, d core.Digest) error {
	logger.Debugw("downloading blob via scheduler")

	// Monitor download with context
	done := make(chan error, 1)
	go func() {
		done <- s.sched.Download(namespace, d)
	}()

	select {
	case err := <-done:
		if err != nil {
			if err == scheduler.ErrTorrentNotFound {
				logger.Debugw("torrent not found", "error", err)
				return handler.ErrorStatus(http.StatusNotFound)
			}
			logger.Errorw("download failed", "error", err)
			return handler.Errorf("download torrent: %s", err)
		}
		logger.Debugw("download completed successfully")
		return nil

	case <-ctx.Done():
		logger.Warnw("download timeout", "timeout", s.config.DownloadTimeout)
		return handler.Errorf("download timeout after %v", s.config.DownloadTimeout).Status(http.StatusRequestTimeout)
	}
}

func (s *Server) deleteBlobHandler(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	logger := s.getLogger(ctx)

	d, err := parseDigest(r)
	if err != nil {
		return err
	}

	logger = logger.With("digest", d.String())
	logger.Debugw("deleting blob")

	if err := s.sched.RemoveTorrent(d); err != nil {
		logger.Errorw("failed to remove torrent", "error", err)
		return handler.Errorf("remove torrent: %s", err)
	}
	logger.Debugw("blob deleted successfully")
	return nil
}

// preloadTagHandler triggers docker daemon to download specified docker image.
func (s *Server) preloadTagHandler(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	logger := s.getLogger(ctx)

	// Create timeout context for container runtime operations
	runtimeCtx, cancel := context.WithTimeout(ctx, s.config.ContainerRuntimeTimeout)
	defer cancel()

	tag, err := httputil.ParseParam(r, "tag")
	if err != nil {
		return handler.Errorf("parse tag param: %s", err).Status(http.StatusBadRequest)
	}

	parts := strings.Split(tag, ":")
	if len(parts) != 2 {
		return handler.Errorf("invalid docker image tag format: %s", tag).Status(http.StatusBadRequest)
	}
	repo, tagName := parts[0], parts[1]

	// Validate inputs
	if strings.TrimSpace(repo) == "" || strings.TrimSpace(tagName) == "" {
		return handler.ErrorStatus(http.StatusBadRequest)
	}

	rt := httputil.GetQueryArg(r, "runtime", "docker")
	ns := httputil.GetQueryArg(r, "namespace", "")

	logger = logger.With("repo", repo, "tag", tagName, "runtime", rt, "namespace", ns)
	logger.Debugw("preloading image")

	var preloadErr error
	switch rt {
	case "docker":
		preloadErr = s.containerRuntime.DockerClient().PullImage(runtimeCtx, repo, tagName)
	case "containerd":
		preloadErr = s.containerRuntime.ContainerdClient().PullImage(runtimeCtx, ns, repo, tagName)
	default:
		return handler.Errorf("unsupported container runtime: %s", rt)
	}

	if preloadErr != nil {
		// Check if it's a context timeout
		if runtimeCtx.Err() == context.DeadlineExceeded {
			logger.Warnw("preload timeout", "timeout", s.config.ContainerRuntimeTimeout)
			return handler.Errorf("preload timeout after %v", s.config.ContainerRuntimeTimeout).Status(http.StatusRequestTimeout)
		}

		logger.Errorw("preload failed", "error", preloadErr)
		return handler.Errorf("%s pull: %s", rt, preloadErr)
	}

	logger.Debugw("preload completed successfully")
	return nil
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	logger := s.getLogger(ctx)

	logger.Debugw("health check")

	if err := s.sched.Probe(); err != nil {
		logger.Errorw("health check failed", "error", err)
		return handler.Errorf("probe torrent client: %s", err)
	}

	io.WriteString(w, "OK")
	return nil
}

func (s *Server) readinessCheckHandler(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	logger := s.getLogger(ctx)

	// Check cache first
	s.lastReadyMu.RLock()
	if s.config.ReadinessCacheTTL != 0 {
		rCacheValid := s.lastReady.Add(s.config.ReadinessCacheTTL).After(time.Now())
		if rCacheValid {
			s.lastReadyMu.RUnlock()
			logger.Debugw("readiness check cached")
			io.WriteString(w, "OK")
			return nil
		}
	}
	s.lastReadyMu.RUnlock()

	logger.Debugw("performing readiness check")

	// Create timeout context for readiness checks
	checkCtx, cancel := context.WithTimeout(ctx, s.config.ReadinessTimeout)
	defer cancel()

	type checkResult struct {
		name string
		err  error
	}

	results := make(chan checkResult, 3)

	// Run checks concurrently with timeout
	go func() {
		err := s.sched.Probe()
		select {
		case results <- checkResult{"scheduler", err}:
		case <-checkCtx.Done():
		}
	}()

	go func() {
		err := s.tags.CheckReadiness()
		select {
		case results <- checkResult{"build-index", err}:
		case <-checkCtx.Done():
		}
	}()

	go func() {
		err := s.ac.CheckReadiness()
		select {
		case results <- checkResult{"tracker", err}:
		case <-checkCtx.Done():
		}
	}()

	// Collect results
	var errMsgs []string
	for i := 0; i < 3; i++ {
		select {
		case result := <-results:
			if result.err != nil {
				errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", result.name, result.err))
			}
		case <-checkCtx.Done():
			logger.Warnw("readiness check timeout", "timeout", s.config.ReadinessTimeout)
			return handler.Errorf("readiness check timeout after %v", s.config.ReadinessTimeout).Status(http.StatusServiceUnavailable)
		}
	}

	if len(errMsgs) != 0 {
		// Sort error messages for deterministic output
		sort.Strings(errMsgs)
		errMsg := strings.Join(errMsgs, "\n")
		logger.Warnw("readiness check failed", "errors", errMsg)
		return handler.Errorf("agent not ready: %v", errMsg).Status(http.StatusServiceUnavailable)
	}

	// Update cache
	s.lastReadyMu.Lock()
	s.lastReady = time.Now()
	s.lastReadyMu.Unlock()

	logger.Debugw("readiness check passed")
	io.WriteString(w, "OK")
	return nil
}

// patchSchedulerConfigHandler restarts the agent torrent scheduler with
// the config in request body.
func (s *Server) patchSchedulerConfigHandler(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	logger := s.getLogger(ctx)
	defer r.Body.Close()

	logger.Debugw("patching scheduler config")

	var config scheduler.Config
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		logger.Errorw("failed to decode scheduler config", "error", err)
		return handler.Errorf("json decode: %s", err).Status(http.StatusBadRequest)
	}

	s.sched.Reload(config)
	logger.Infow("scheduler config reloaded")
	return nil
}

func (s *Server) getBlacklistHandler(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	logger := s.getLogger(ctx)

	logger.Debugw("getting blacklist")

	blacklist, err := s.sched.BlacklistSnapshot()
	if err != nil {
		logger.Errorw("failed to get blacklist", "error", err)
		return handler.Errorf("blacklist snapshot: %s", err)
	}

	if err := json.NewEncoder(w).Encode(&blacklist); err != nil {
		logger.Errorw("failed to encode blacklist", "error", err)
		return handler.Errorf("json encode: %s", err)
	}

	logger.Debugw("blacklist retrieved", "count", len(blacklist))
	return nil
}

func parseDigest(r *http.Request) (core.Digest, error) {
	raw, err := httputil.ParseParam(r, "digest")
	if err != nil {
		return core.Digest{}, handler.Errorf("parse digest param: %s", err).Status(http.StatusBadRequest)
	}

	// Validate digest format
	if strings.TrimSpace(raw) == "" {
		return core.Digest{}, handler.ErrorStatus(http.StatusBadRequest)
	}

	// TODO(codyg): Accept only a fully formed digest.
	d, err := core.NewSHA256DigestFromHex(raw)
	if err != nil {
		d, err = core.ParseSHA256Digest(raw)
		if err != nil {
			return core.Digest{}, handler.Errorf("parse digest: %s", err).Status(http.StatusBadRequest)
		}
	}
	return d, nil
}
