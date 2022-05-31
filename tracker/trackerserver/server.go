// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package trackerserver

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.

	"github.com/go-chi/chi"
	chimiddleware "github.com/go-chi/chi/middleware"
	"github.com/uber-go/tally"

	"github.com/uber/kraken/lib/middleware"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/tracker/originstore"
	"github.com/uber/kraken/tracker/peerhandoutpolicy"
	"github.com/uber/kraken/tracker/peerstore"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/listener"
	"github.com/uber/kraken/utils/log"
)

// Server serves Tracker endpoints.
type Server struct {
	config Config
	stats  tally.Scope

	peerStore   peerstore.Store
	originStore originstore.Store
	policy      *peerhandoutpolicy.PriorityPolicy

	originCluster blobclient.ClusterClient
}

// New creates a new Server.
func New(
	config Config,
	stats tally.Scope,
	policy *peerhandoutpolicy.PriorityPolicy,
	peerStore peerstore.Store,
	originStore originstore.Store,
	originCluster blobclient.ClusterClient) *Server {

	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "trackerserver",
	})

	return &Server{
		config:        config,
		stats:         stats,
		peerStore:     peerStore,
		originStore:   originStore,
		policy:        policy,
		originCluster: originCluster,
	}
}

// Handler an http handler for s.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))
	r.Get("/announce", handler.Wrap(s.announceHandlerV1))
	r.Post("/announce/{infohash}", handler.Wrap(s.announceHandlerV2))
	r.Get("/namespace/{namespace}/blobs/{digest}/metainfo", handler.Wrap(s.getMetaInfoHandler))

	r.Mount("/debug", chimiddleware.Profiler())

	return r
}

// ListenAndServe is a blocking call which runs s.
func (s *Server) ListenAndServe() error {
	log.Infof("Starting tracker server on %s", s.config.Listener)
	return listener.Serve(s.config.Listener, s.Handler())
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	fmt.Fprintln(w, "OK")
	return nil
}
