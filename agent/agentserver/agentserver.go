package agentserver

import (
	"fmt"
	"io"
	"net/http"

	"github.com/pressly/chi"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
)

// Server defines the agent HTTP server.
type Server struct {
	config        Config
	fs            store.FileStore
	torrentClient torrent.Client
}

// New creates a new Server.
func New(config Config, fs store.FileStore, torrentClient torrent.Client) *Server {
	return &Server{config, fs, torrentClient}
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Get("/blobs/:name", s.getBlobHandler)

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
	blob, err := s.torrentClient.DownloadTorrent(name)
	if err != nil {
		http.Error(w, fmt.Sprintf("download torrent: %s", err), http.StatusInternalServerError)
		return
	}
	if err := s.fs.CreateCacheFile(name, blob); err != nil {
		http.Error(w, fmt.Sprintf("create cache file: %s", err), http.StatusInternalServerError)
		return
	}
	filepath, err := s.fs.GetCacheFilePath(name)
	if err != nil {
		http.Error(w, fmt.Sprintf("get file path: %s", err), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, filepath)
}
