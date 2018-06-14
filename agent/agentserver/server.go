package agentserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.
	"os"

	"github.com/pressly/chi"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Config defines Server configuration.
type Config struct{}

// Server defines the agent HTTP server.
type Server struct {
	config Config
	stats  tally.Scope
	fs     store.FileStore
	sched  scheduler.ReloadableScheduler
}

// New creates a new Server.
func New(
	config Config,
	stats tally.Scope,
	fs store.FileStore,
	sched scheduler.ReloadableScheduler) *Server {

	stats = stats.Tagged(map[string]string{
		"module": "agentserver",
	})
	return &Server{config, stats, fs, sched}
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))

	r.Get("/namespace/:namespace/blobs/:name", handler.Wrap(s.downloadBlobHandler))

	r.Delete("/blobs/:name", handler.Wrap(s.deleteBlobHandler))

	// Dangerous endpoint for running experiments.
	r.Patch("/x/config/scheduler", handler.Wrap(s.patchSchedulerConfigHandler))

	r.Get("/x/blacklist", handler.Wrap(s.getBlacklistHandler))

	// Serves /debug/pprof endpoints.
	r.Mount("/", http.DefaultServeMux)

	return r
}

// downloadBlobHandler downloads a blob through p2p.
func (s *Server) downloadBlobHandler(w http.ResponseWriter, r *http.Request) error {
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	name, err := httputil.ParseParam(r, "name")
	if err != nil {
		return err
	}

	f, err := s.fs.GetCacheFileReader(name)
	if err != nil {
		if os.IsNotExist(err) || s.fs.InDownloadError(err) {
			if err := s.sched.Download(namespace, name); err != nil {
				if err == scheduler.ErrTorrentNotFound {
					return handler.ErrorStatus(http.StatusNotFound)
				}
				return handler.Errorf("download torrent: %s", err)
			}
			f, err = s.fs.GetCacheFileReader(name)
			if err != nil {
				return handler.Errorf("file store: %s", err)
			}
		} else {
			return handler.Errorf("file store: %s", err)
		}
	}
	if _, err := io.Copy(w, f); err != nil {
		return fmt.Errorf("copy file: %s", err)
	}
	return nil
}

func (s *Server) deleteBlobHandler(w http.ResponseWriter, r *http.Request) error {
	name, err := httputil.ParseParam(r, "name")
	if err != nil {
		return err
	}

	if err := s.sched.RemoveTorrent(name); err != nil {
		return handler.Errorf("remove torrent: %s", err)
	}
	return nil
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	if err := s.sched.Probe(); err != nil {
		return handler.Errorf("probe torrent client: %s", err)
	}
	fmt.Fprintln(w, "OK")
	return nil
}

// patchSchedulerConfigHandler restarts the agent torrent scheduler with
// the config in request body.
func (s *Server) patchSchedulerConfigHandler(w http.ResponseWriter, r *http.Request) error {
	defer r.Body.Close()
	var config scheduler.Config
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		return handler.Errorf("json decode: %s", err).Status(http.StatusBadRequest)
	}
	s.sched.Reload(config)
	return nil
}

func (s *Server) getBlacklistHandler(w http.ResponseWriter, r *http.Request) error {
	blacklist, err := s.sched.BlacklistSnapshot()
	if err != nil {
		return handler.Errorf("blacklist snapshot: %s", err)
	}
	if err := json.NewEncoder(w).Encode(&blacklist); err != nil {
		return handler.Errorf("json encode: %s", err)
	}
	return nil
}
