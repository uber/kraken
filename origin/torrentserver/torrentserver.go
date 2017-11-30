package torrentserver

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pressly/chi"

	"code.uber.internal/infra/kraken/lib/torrent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
)

// Server wraps operational endpoints for the origin's torrent client.
type Server struct {
	torrentClient torrent.Client
}

// New returns a new Server.
func New(torrentClient torrent.Client) *Server {
	return &Server{torrentClient}
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	// Dangerous endpoint for running experiments.
	r.Patch("/x/config/scheduler", s.patchSchedulerConfigHandler)

	return r
}

// patchSchedulerConfigHandler restarts the torrent scheduler with
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
