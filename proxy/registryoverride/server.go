package registryoverride

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/listener"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/stringset"
	"github.com/pressly/chi"
)

// Server overrides Docker registry endpoints.
type Server struct {
	config    Config
	tagClient tagclient.Client
}

// NewServer creates a new Server.
func NewServer(config Config, tagClient tagclient.Client) *Server {
	return &Server{config, tagClient}
}

// Handler returns a handler for s.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Get("/v2/_catalog", handler.Wrap(s.catalogHandler))
	return r
}

// ListenAndServe is a blocking call which runs s.
func (s *Server) ListenAndServe() error {
	log.Infof("Starting registry override server on %s", s.config.Listener)
	return listener.Serve(s.config.Listener, s.Handler())
}

type catalogResponse struct {
	Repositories []string `json:"repositories"`
}

func (s *Server) catalogHandler(w http.ResponseWriter, r *http.Request) error {
	tags, err := s.tagClient.List("")
	if err != nil {
		return handler.Errorf("list: %s", err)
	}
	repos := stringset.New()
	for _, tag := range tags {
		parts := strings.Split(tag, ":")
		if len(parts) != 2 {
			log.With("tag", tag).Errorf("Invalid tag format, expected repo:tag")
			continue
		}
		repos.Add(parts[0])
	}
	resp := catalogResponse{Repositories: repos.ToSlice()}
	if err := json.NewEncoder(w).Encode(&resp); err != nil {
		return handler.Errorf("json encode: %s", err)
	}
	return nil
}
