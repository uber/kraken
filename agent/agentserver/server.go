package agentserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.
	"os"

	"github.com/andres-erbsen/clock"
	"github.com/pressly/chi"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/handler"
)

// Config defines Server configuration.
type Config struct {
	RequestCache dedup.RequestCacheConfig `yaml:"request_cache"`
}

// Server defines the agent HTTP server.
type Server struct {
	config        Config
	stats         tally.Scope
	fs            store.FileStore
	torrentClient torrent.Client
	requestCache  *dedup.RequestCache
}

// New creates a new Server.
func New(
	config Config, stats tally.Scope, fs store.FileStore, tc torrent.Client) *Server {

	stats = stats.Tagged(map[string]string{
		"module": "agentserver",
	})

	rc := dedup.NewRequestCache(config.RequestCache, clock.New())
	rc.SetNotFound(func(err error) bool { return err == scheduler.ErrTorrentNotFound })

	return &Server{config, stats, fs, tc, rc}
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.HitCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", s.healthHandler)

	r.Get("/namespace/:namespace/blobs/:name", handler.Wrap(s.downloadBlobHandler))

	// Dangerous endpoint for running experiments.
	r.Patch("/x/config/scheduler", handler.Wrap(s.patchSchedulerConfigHandler))

	r.Get("/x/blacklist", handler.Wrap(s.getBlacklistHandler))

	// Serves /debug/pprof endpoints.
	r.Mount("/", http.DefaultServeMux)

	return r
}

// downloadBlobHandler initiates a p2p download of a blob. This is a non-blocking
// endpoint, which returns 202 while the download is still in progress.
func (s *Server) downloadBlobHandler(w http.ResponseWriter, r *http.Request) error {
	namespace := chi.URLParam(r, "namespace")
	if namespace == "" {
		return handler.Errorf("namespace required").Status(http.StatusBadRequest)
	}
	name := chi.URLParam(r, "name")
	if name == "" {
		return handler.Errorf("name required").Status(http.StatusBadRequest)
	}
	f, err := s.fs.GetCacheFileReader(name)
	if err != nil {
		if os.IsNotExist(err) || s.fs.InDownloadError(err) {
			return s.startTorrentDownload(namespace, name)
		}
		return handler.Errorf("file store: %s", err)
	}
	if _, err := io.Copy(w, f); err != nil {
		return fmt.Errorf("copy file: %s", err)
	}
	return nil
}

func (s *Server) startTorrentDownload(namespace, name string) error {
	id := namespace + ":" + name
	err := s.requestCache.Start(id, func() error {
		return s.torrentClient.Download(namespace, name)
	})
	switch err {
	case dedup.ErrRequestPending, nil:
		return handler.ErrorStatus(http.StatusAccepted)
	case dedup.ErrWorkersBusy:
		return handler.ErrorStatus(http.StatusServiceUnavailable)
	case scheduler.ErrTorrentNotFound:
		return handler.ErrorStatus(http.StatusNotFound)
	default:
		return handler.Errorf("download torrent: %s", err)
	}
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "OK")
}

// patchSchedulerConfigHandler restarts the agent torrent scheduler with
// the config in request body.
func (s *Server) patchSchedulerConfigHandler(w http.ResponseWriter, r *http.Request) error {
	defer r.Body.Close()
	var config scheduler.Config
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		return handler.Errorf("json decode: %s", err).Status(http.StatusBadRequest)
	}
	s.torrentClient.Reload(config)
	return nil
}

func (s *Server) getBlacklistHandler(w http.ResponseWriter, r *http.Request) error {
	blacklist, err := s.torrentClient.BlacklistSnapshot()
	if err != nil {
		return handler.Errorf("blacklist snapshot: %s", err)
	}
	if err := json.NewEncoder(w).Encode(&blacklist); err != nil {
		return handler.Errorf("json encode: %s", err)
	}
	return nil
}
