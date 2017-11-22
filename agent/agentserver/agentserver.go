package agentserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/pressly/chi"

	"code.uber.internal/infra/kraken/lib/torrent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
)

// Server defines the agent HTTP server.
type Server struct {
	config        Config
	torrentClient torrent.Client
}

// New creates a new Server.
func New(config Config, torrentClient torrent.Client) *Server {
	return &Server{config, torrentClient}
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Get("/blobs/:name", s.getBlobHandler)
	r.Get("/health", s.healthHandler)

	// Dangerous endpoint for running experiments.
	r.Patch("/x/config/scheduler", s.patchSchedulerConfigHandler)

	return r
}

// getBlobHandler downloads blobs into the agent cache. Returns the filepath of
// the blob in the response body.
func (s *Server) getBlobHandler(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}
	if _, err := s.torrentClient.Download(name); err != nil {
		http.Error(w, fmt.Sprintf("download torrent: %s", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "OK")
}

// patchSchedulerConfigHandler restarts the agent torrent scheduler with
// the config in request body.
func (s *Server) patchSchedulerConfigHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var config scheduler.Config
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		http.Error(w, fmt.Sprintf("decode body: %s", err), http.StatusBadRequest)
		return
	}
	s.torrentClient.Reload(config)
}
