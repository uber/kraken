// Copyright (c) 2019 Uber Technologies, Inc.
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
// limitations under the License.package agentserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.
	"os"

	"github.com/pressly/chi"
	"github.com/uber-go/tally"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/middleware"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/httputil"
)

// Config defines Server configuration.
type Config struct{}

// Server defines the agent HTTP server.
type Server struct {
	config Config
	stats  tally.Scope
	cads   *store.CADownloadStore
	sched  scheduler.ReloadableScheduler
}

// New creates a new Server.
func New(
	config Config,
	stats tally.Scope,
	cads *store.CADownloadStore,
	sched scheduler.ReloadableScheduler) *Server {

	stats = stats.Tagged(map[string]string{
		"module": "agentserver",
	})
	return &Server{config, stats, cads, sched}
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))

	r.Get("/namespace/:namespace/blobs/:digest", handler.Wrap(s.downloadBlobHandler))

	r.Delete("/blobs/:digest", handler.Wrap(s.deleteBlobHandler))

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
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	f, err := s.cads.Cache().GetFileReader(d.Hex())
	if err != nil {
		if os.IsNotExist(err) || s.cads.InDownloadError(err) {
			if err := s.sched.Download(namespace, d); err != nil {
				if err == scheduler.ErrTorrentNotFound {
					return handler.ErrorStatus(http.StatusNotFound)
				}
				return handler.Errorf("download torrent: %s", err)
			}
			f, err = s.cads.Cache().GetFileReader(d.Hex())
			if err != nil {
				return handler.Errorf("store: %s", err)
			}
		} else {
			return handler.Errorf("store: %s", err)
		}
	}
	if _, err := io.Copy(w, f); err != nil {
		return fmt.Errorf("copy file: %s", err)
	}
	return nil
}

func (s *Server) deleteBlobHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	if err := s.sched.RemoveTorrent(d); err != nil {
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

func parseDigest(r *http.Request) (core.Digest, error) {
	raw, err := httputil.ParseParam(r, "digest")
	if err != nil {
		return core.Digest{}, err
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
