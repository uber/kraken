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
package tagserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/build-index/tagmodels"
	"github.com/uber/kraken/build-index/tagstore"
	"github.com/uber/kraken/build-index/tagtype"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/lib/middleware"
	"github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/lib/persistedretry/tagreplication"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/listener"
	"github.com/uber/kraken/utils/log"

	"github.com/go-chi/chi"
	chimiddleware "github.com/go-chi/chi/middleware"
	"github.com/uber-go/tally"
)

// Server provides tag operations for the build-index.
type Server struct {
	config            Config
	stats             tally.Scope
	backends          *backend.Manager
	localOriginDNS    string
	localOriginClient blobclient.ClusterClient
	neighbors         hostlist.List
	store             tagstore.Store

	// For async new tag replication.
	remotes               tagreplication.Remotes
	tagReplicationManager persistedretry.Manager
	provider              tagclient.Provider

	// For checking if a tag has all dependent blobs.
	depResolver tagtype.DependencyResolver
}

// New creates a new Server.
func New(
	config Config,
	stats tally.Scope,
	backends *backend.Manager,
	localOriginDNS string,
	localOriginClient blobclient.ClusterClient,
	neighbors hostlist.List,
	store tagstore.Store,
	remotes tagreplication.Remotes,
	tagReplicationManager persistedretry.Manager,
	provider tagclient.Provider,
	depResolver tagtype.DependencyResolver) *Server {

	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "tagserver",
	})

	return &Server{
		config:                config,
		stats:                 stats,
		backends:              backends,
		localOriginDNS:        localOriginDNS,
		localOriginClient:     localOriginClient,
		neighbors:             neighbors,
		store:                 store,
		remotes:               remotes,
		tagReplicationManager: tagReplicationManager,
		provider:              provider,
		depResolver:           depResolver,
	}
}

// Handler returns an http.Handler for s.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))

	r.Put("/tags/{tag}/digest/{digest}", handler.Wrap(s.putTagHandler))
	r.Head("/tags/{tag}", handler.Wrap(s.hasTagHandler))
	r.Get("/tags/{tag}", handler.Wrap(s.getTagHandler))

	r.Get("/repositories/{repo}/tags", handler.Wrap(s.listRepositoryHandler))

	r.Get("/list/*", handler.Wrap(s.listHandler))

	r.Post("/remotes/tags/{tag}", handler.Wrap(s.replicateTagHandler))

	r.Get("/origin", handler.Wrap(s.getOriginHandler))

	r.Post(
		"/internal/duplicate/remotes/tags/{tag}/digest/{digest}",
		handler.Wrap(s.duplicateReplicateTagHandler))

	r.Put(
		"/internal/duplicate/tags/{tag}/digest/{digest}",
		handler.Wrap(s.duplicatePutTagHandler))

	r.Mount("/debug", chimiddleware.Profiler())

	return r
}

// ListenAndServe is a blocking call which runs s.
func (s *Server) ListenAndServe() error {
	log.Infof("Starting tag server on %s", s.config.Listener)
	return listener.Serve(s.config.Listener, s.Handler())
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	fmt.Fprintln(w, "OK")
	return nil
}

func (s *Server) putTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := httputil.ParseParam(r, "tag")
	if err != nil {
		return err
	}
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	replicate, err := strconv.ParseBool(httputil.GetQueryArg(r, "replicate", "false"))
	if err != nil {
		return handler.Errorf("parse query arg `replicate`: %s", err)
	}

	deps, err := s.depResolver.Resolve(tag, d)
	if err != nil {
		return fmt.Errorf("resolve dependencies: %s", err)
	}
	if err := s.putTag(tag, d, deps); err != nil {
		return err
	}

	if replicate {
		if err := s.replicateTag(tag, d, deps); err != nil {
			return err
		}
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) duplicatePutTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := httputil.ParseParam(r, "tag")
	if err != nil {
		return err
	}
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}

	var req tagclient.DuplicatePutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return handler.Errorf("decode body: %s", err)
	}
	delay := req.Delay

	if err := s.store.Put(tag, d, delay); err != nil {
		return handler.Errorf("storage: %s", err)
	}

	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) getTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := httputil.ParseParam(r, "tag")
	if err != nil {
		return err
	}

	d, err := s.store.Get(tag)
	if err != nil {
		if err == tagstore.ErrTagNotFound {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("storage: %s", err)
	}

	if _, err := io.WriteString(w, d.String()); err != nil {
		return handler.Errorf("write digest: %s", err)
	}
	return nil
}

func (s *Server) hasTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := httputil.ParseParam(r, "tag")
	if err != nil {
		return err
	}

	client, err := s.backends.GetClient(tag)
	if err != nil {
		return handler.Errorf("backend manager: %s", err)
	}
	if _, err := client.Stat(tag, tag); err != nil {
		if err == backenderrors.ErrBlobNotFound {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return err
	}
	return nil
}

// listHandler handles list images request. Response model
// tagmodels.ListResponse.
func (s *Server) listHandler(w http.ResponseWriter, r *http.Request) error {
	prefix := r.URL.Path[len("/list/"):]

	client, err := s.backends.GetClient(prefix)
	if err != nil {
		return handler.Errorf("backend manager: %s", err)
	}

	opts, err := buildPaginationOptions(r.URL)
	if err != nil {
		return err
	}

	result, err := client.List(prefix, opts...)
	if err != nil {
		return handler.Errorf("error listing from backend: %s", err)
	}

	resp, err := buildPaginationResponse(r.URL, result.ContinuationToken,
		result.Names)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return handler.Errorf("json encode: %s", err)
	}
	return nil
}

// listRepositoryHandler handles list images tag request. Response model
// tagmodels.ListResponse.
// TODO(codyg): Remove this.
func (s *Server) listRepositoryHandler(w http.ResponseWriter, r *http.Request) error {
	repo, err := httputil.ParseParam(r, "repo")
	if err != nil {
		return err
	}

	client, err := s.backends.GetClient(repo)
	if err != nil {
		return handler.Errorf("backend manager: %s", err)
	}

	opts, err := buildPaginationOptions(r.URL)
	if err != nil {
		return err
	}

	result, err := client.List(path.Join(repo, "_manifests/tags"), opts...)
	if err != nil {
		return handler.Errorf("error listing from backend: %s", err)
	}

	var tags []string
	for _, name := range result.Names {
		// Strip repo prefix.
		parts := strings.Split(name, ":")
		if len(parts) != 2 {
			log.With("name", name).Warn("Repo list skipping name, expected repo:tag format")
			continue
		}
		tags = append(tags, parts[1])
	}

	resp, err := buildPaginationResponse(r.URL, result.ContinuationToken, tags)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return handler.Errorf("json encode: %s", err)
	}
	return nil
}

func (s *Server) replicateTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := httputil.ParseParam(r, "tag")
	if err != nil {
		return err
	}

	d, err := s.store.Get(tag)
	if err != nil {
		if err == tagstore.ErrTagNotFound {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("storage: %s", err)
	}
	deps, err := s.depResolver.Resolve(tag, d)
	if err != nil {
		return fmt.Errorf("resolve dependencies: %s", err)
	}
	if err := s.replicateTag(tag, d, deps); err != nil {
		return err
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) duplicateReplicateTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := httputil.ParseParam(r, "tag")
	if err != nil {
		return err
	}
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return handler.Errorf("get dependency resolver: %s", err)
	}
	var req tagclient.DuplicateReplicateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return handler.Errorf("decode body: %s", err)
	}

	destinations := s.remotes.Match(tag)

	for _, dest := range destinations {
		task := tagreplication.NewTask(tag, d, req.Dependencies, dest, req.Delay)
		if err := s.tagReplicationManager.Add(task); err != nil {
			return handler.Errorf("add replicate task: %s", err)
		}
	}

	return nil
}

func (s *Server) getOriginHandler(w http.ResponseWriter, r *http.Request) error {
	if _, err := io.WriteString(w, s.localOriginDNS); err != nil {
		return handler.Errorf("write local origin dns: %s", err)
	}
	return nil
}

func (s *Server) putTag(tag string, d core.Digest, deps core.DigestList) error {
	for _, dep := range deps {
		if _, err := s.localOriginClient.Stat(tag, dep); err == blobclient.ErrBlobNotFound {
			return handler.Errorf("cannot upload tag, missing dependency %s", dep)
		} else if err != nil {
			return handler.Errorf("check blob: %s", err)
		}
	}

	if err := s.store.Put(tag, d, 0); err != nil {
		return handler.Errorf("storage: %s", err)
	}

	neighbors := s.neighbors.Resolve()

	var delay time.Duration
	var successes int
	for addr := range neighbors {
		delay += s.config.DuplicatePutStagger
		client := s.provider.Provide(addr)
		if err := client.DuplicatePut(tag, d, delay); err != nil {
			log.Errorf("Error duplicating put task to %s: %s", addr, err)
		} else {
			successes++
		}
	}
	if len(neighbors) != 0 && successes == 0 {
		s.stats.Counter("duplicate_put_failures").Inc(1)
	}
	return nil
}

func (s *Server) replicateTag(tag string, d core.Digest, deps core.DigestList) error {
	destinations := s.remotes.Match(tag)
	if len(destinations) == 0 {
		return nil
	}

	for _, dest := range destinations {
		task := tagreplication.NewTask(tag, d, deps, dest, 0)
		if err := s.tagReplicationManager.Add(task); err != nil {
			return handler.Errorf("add replicate task: %s", err)
		}
	}

	neighbors := s.neighbors.Resolve()

	var delay time.Duration
	var successes int
	for addr := range neighbors { // Loops in random order.
		delay += s.config.DuplicateReplicateStagger
		client := s.provider.Provide(addr)
		if err := client.DuplicateReplicate(tag, d, deps, delay); err != nil {
			log.Errorf("Error duplicating replicate task to %s: %s", addr, err)
		} else {
			successes++
		}
	}
	if len(neighbors) != 0 && successes == 0 {
		s.stats.Counter("duplicate_replicate_failures").Inc(1)
	}
	return nil
}

func buildPaginationOptions(u *url.URL) ([]backend.ListOption, error) {
	var opts []backend.ListOption
	q := u.Query()
	for k, v := range q {
		if len(v) != 1 {
			return nil, handler.Errorf(
				"invalid query %s:%s", k, v).Status(http.StatusBadRequest)
		}
		switch k {
		case tagmodels.LimitQ:
			limitCount, err := strconv.Atoi(v[0])
			if err != nil {
				return nil, handler.Errorf(
					"invalid limit %s: %s", v, err).Status(http.StatusBadRequest)
			}
			if limitCount == 0 {
				return nil, handler.Errorf(
					"invalid limit %d", limitCount).Status(http.StatusBadRequest)
			}
			opts = append(opts, backend.ListWithMaxKeys(limitCount))
		case tagmodels.OffsetQ:
			opts = append(opts, backend.ListWithContinuationToken(v[0]))
		default:
			return nil, handler.Errorf(
				"invalid query %s", k).Status(http.StatusBadRequest)
		}
	}
	if len(opts) > 0 {
		// Enable pagination if either or both of the query param exists.
		opts = append(opts, backend.ListWithPagination())
	}

	return opts, nil
}

func buildPaginationResponse(u *url.URL, continuationToken string,
	result []string) (*tagmodels.ListResponse, error) {

	nextUrlString := ""
	if continuationToken != "" {
		// Deep copy url.
		nextUrl, err := url.Parse(u.String())
		if err != nil {
			return nil, handler.Errorf(
				"invalid url string: %s", err).Status(http.StatusBadRequest)
		}
		v := url.Values{}
		if limit := u.Query().Get(tagmodels.LimitQ); limit != "" {
			v.Add(tagmodels.LimitQ, limit)
		}
		// ContinuationToken cannot be empty here.
		v.Add(tagmodels.OffsetQ, continuationToken)
		nextUrl.RawQuery = v.Encode()
		nextUrlString = nextUrl.String()
	}

	resp := tagmodels.ListResponse{
		Size:   len(result),
		Result: result,
	}
	resp.Links.Next = nextUrlString
	resp.Links.Self = u.String()

	return &resp, nil
}
