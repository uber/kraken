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
package proxyserver

import (
	"fmt"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.

	"github.com/go-chi/chi"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/lib/middleware"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/handler"
)

// Server defines the proxy HTTP server.
type Server struct {
	stats          tally.Scope
	preheatHandler *PreheatHandler
}

// New creates a new Server.
func New(
	stats tally.Scope,
	client blobclient.ClusterClient) *Server {

	return &Server{
		stats.Tagged(map[string]string{"module": "proxyserver"}),
		NewPreheatHandler(client)}
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))

	r.Post("/registry/notifications", handler.Wrap(s.preheatHandler.Handle))

	// Serves /debug/pprof endpoints.
	r.Mount("/", http.DefaultServeMux)

	return r
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	fmt.Fprintln(w, "OK")
	return nil
}
