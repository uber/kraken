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
	"strings"
	"sync"
	"time"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/containerruntime"
	"github.com/uber/kraken/lib/middleware"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/torrent/scheduler"
	"github.com/uber/kraken/tracker/announceclient"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/httputil"

	"github.com/go-chi/chi"
	"github.com/uber-go/tally"
)

// Config defines Server configuration.
type Config struct {
	// How long a successful readiness check is valid for. If 0, disable caching successful readiness.
	readinessCacheTTL time.Duration `yaml:"readiness_cache_ttl"`
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

// getTagHandler proxies get tag requests to the build-index.
func (s *Server) getTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := httputil.ParseParam(r, "tag")
	if err != nil {
		return err
	}

	d, err := s.tags.Get(tag)
	if err == tagclient.ErrTagNotFound {
		return handler.ErrorStatus(http.StatusNotFound)
	}
	if err != nil {
		return handler.Errorf("get tag: %s", err)
	}

	io.WriteString(w, d.String())
	return nil
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

	// Happy path: file already exists in cache
	if err == nil {
		defer closers.Close(f)
		if _, err := io.Copy(w, f); err != nil {
			return fmt.Errorf("copy file: %w", err)
		}
		return nil
	}

	// If error is not recoverable, return error
	if !os.IsNotExist(err) && !s.cads.InDownloadError(err) {
		return handler.Errorf("store: %s", err)
	}

	// File doesn't exist or is in wrong state, trigger P2P download
	if err := s.sched.Download(namespace, d); err != nil {
		if err == scheduler.ErrTorrentNotFound {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("download torrent: %s", err)
	}

	// Get file reader after download completes
	// Use Any() to check both download and cache directories, as the file
	// might still be in the process of being moved from download to cache.
	f, err = s.cads.Any().GetFileReader(d.Hex())
	if err != nil {
		return handler.Errorf("store: %s", err)
	}
	defer closers.Close(f)

	if _, err := io.Copy(w, f); err != nil {
		return fmt.Errorf("copy file: %w", err)
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

// preloadTagHandler triggers docker daemon to download specified docker image.
func (s *Server) preloadTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := httputil.ParseParam(r, "tag")
	if err != nil {
		return err
	}
	parts := strings.Split(tag, ":")
	if len(parts) != 2 {
		return handler.Errorf("failed to parse docker image tag")
	}
	repo, tag := parts[0], parts[1]

	rt := httputil.GetQueryArg(r, "runtime", "docker")
	ns := httputil.GetQueryArg(r, "namespace", "")
	switch rt {
	case "docker":
		if err := s.containerRuntime.DockerClient().
			PullImage(context.Background(), repo, tag); err != nil {
			return handler.Errorf("docker pull: %s", err)
		}
	case "containerd":
		if err := s.containerRuntime.ContainerdClient().
			PullImage(context.Background(), ns, repo, tag); err != nil {
			return handler.Errorf("containerd pull: %s", err)
		}
	default:
		return handler.Errorf("unsupported container runtime")
	}
	return nil
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	if err := s.sched.Probe(); err != nil {
		return handler.Errorf("probe torrent client: %s", err)
	}
	io.WriteString(w, "OK")
	return nil
}

func (s *Server) readinessCheckHandler(w http.ResponseWriter, r *http.Request) error {
	if s.config.readinessCacheTTL != 0 {
		rCacheValid := s.lastReady.Add(s.config.readinessCacheTTL).After(time.Now())
		if rCacheValid {
			io.WriteString(w, "OK")
			return nil
		}
	}

	var schedErr, buildIndexErr, trackerErr error
	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		schedErr = s.sched.Probe()
		wg.Done()
	}()
	go func() {
		buildIndexErr = s.tags.CheckReadiness()
		wg.Done()
	}()
	go func() {
		trackerErr = s.ac.CheckReadiness()
		wg.Done()
	}()
	wg.Wait()

	// TODO(akalpakchiev): Replace with errors.Join once upgraded to Go 1.20+.
	errMsgs := []string{}
	for _, err := range []error{schedErr, buildIndexErr, trackerErr} {
		if err != nil {
			errMsgs = append(errMsgs, err.Error())
		}
	}
	if len(errMsgs) != 0 {
		return handler.Errorf("agent not ready: %v", strings.Join(errMsgs, "\n")).Status(http.StatusServiceUnavailable)
	}

	s.lastReady = time.Now()
	io.WriteString(w, "OK")
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
