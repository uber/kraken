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
	depResolver tagtype.DependencyResolver,
) *Server {
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
	r.Get("/readiness", handler.Wrap(s.readinessCheckHandler))

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
	_, err := fmt.Fprintln(w, "OK")
	if err != nil {
		log.With("error", err).Error("Health check write failed")
		return handler.Errorf("write health check: %s", err)
	}
	return nil
}

func (s *Server) readinessCheckHandler(w http.ResponseWriter, r *http.Request) error {
	err := s.backends.CheckReadiness()
	if err != nil {
		log.With("error", err).Error("Backends readiness check failed")
		return handler.Errorf("not ready to serve traffic: %s", err).Status(http.StatusServiceUnavailable)
	}
	err = s.localOriginClient.CheckReadiness()
	if err != nil {
		log.With("error", err).Error("Origin readiness check failed")
		return handler.Errorf("not ready to serve traffic: %s", err).Status(http.StatusServiceUnavailable)
	}
	_, err = fmt.Fprintln(w, "OK")
	if err != nil {
		log.With("error", err).Error("Readiness check write failed")
		return handler.Errorf("write readiness check: %s", err)
	}
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

	log.With("tag", tag, "digest", d.String(), "replicate", replicate).Info("Putting tag")

	deps, err := s.depResolver.Resolve(tag, d)
	if err != nil {
		log.With("tag", tag, "digest", d.String()).Errorf("Failed to resolve dependencies: %s", err)
		return fmt.Errorf("resolve dependencies: %s", err)
	}

	log.With("tag", tag, "digest", d.String(), "dependency_count", len(deps), "dependencies", digestListToStrings(deps)).Debug("Resolved dependencies")

	if err := s.putTag(tag, d, deps); err != nil {
		log.With("tag", tag, "digest", d.String()).Errorf("Failed to put tag: %s", err)
		return err
	}

	log.With("tag", tag, "digest", d.String()).Info("Successfully put tag")

	if replicate {
		log.With("tag", tag, "digest", d.String()).Info("Starting tag replication")
		if err := s.replicateTag(tag, d, deps); err != nil {
			log.With("tag", tag, "digest", d.String()).Errorf("Failed to replicate tag: %s", err)
			return err
		}
		log.With("tag", tag, "digest", d.String()).Info("Successfully replicated tag")
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

	log.With("tag", tag, "digest", d.String(), "delay", delay).Debug("Received duplicate put request from neighbor")

	if err := s.store.Put(tag, d, delay); err != nil {
		log.With("tag", tag, "digest", d.String(), "delay", delay).Errorf("Failed to store tag from duplicate put: %s", err)
		return handler.Errorf("storage: %s", err)
	}

	log.With("tag", tag, "digest", d.String(), "delay", delay).Info("Successfully stored tag from duplicate put")

	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) getTagHandler(w http.ResponseWriter, r *http.Request) error {
	tag, err := httputil.ParseParam(r, "tag")
	if err != nil {
		return err
	}

	log.With("tag", tag).Debug("Getting tag")

	d, err := s.store.Get(tag)
	if err != nil {
		if err == tagstore.ErrTagNotFound {
			log.With("tag", tag).Debug("Tag not found")
			return handler.ErrorStatus(http.StatusNotFound)
		}
		log.With("tag", tag).Errorf("Failed to get tag from storage: %s", err)
		return handler.Errorf("storage: %s", err)
	}

	log.With("tag", tag, "digest", d.String()).Debug("Successfully retrieved tag")

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

	log.With("tag", tag).Debug("Checking if tag exists")

	client, err := s.backends.GetClient(tag)
	if err != nil {
		log.With("tag", tag).Errorf("Failed to get backend client: %s", err)
		return handler.Errorf("backend manager: %s", err)
	}
	if _, err := client.Stat(tag, tag); err != nil {
		if err == backenderrors.ErrBlobNotFound {
			log.With("tag", tag).Debug("Tag does not exist in backend")
			return handler.ErrorStatus(http.StatusNotFound)
		}
		log.With("tag", tag).Errorf("Failed to check tag existence: %s", err)
		return err
	}

	log.With("tag", tag).Debug("Tag exists in backend")
	return nil
}

// listHandler handles list images request. Response model
// tagmodels.ListResponse.
func (s *Server) listHandler(w http.ResponseWriter, r *http.Request) error {
	prefix := r.URL.Path[len("/list/"):]

	log.With("prefix", prefix).Debug("Listing tags with prefix")

	client, err := s.backends.GetClient(prefix)
	if err != nil {
		log.With("prefix", prefix).Errorf("Failed to get backend client for list: %s", err)
		return handler.Errorf("backend manager: %s", err)
	}

	opts, err := buildPaginationOptions(r.URL)
	if err != nil {
		return err
	}

	result, err := client.List(prefix, opts...)
	if err != nil {
		log.With("prefix", prefix).Errorf("Failed to list from backend: %s", err)
		return handler.Errorf("error listing from backend: %s", err)
	}

	log.With("prefix", prefix, "result_count", len(result.Names), "continuation_token", result.ContinuationToken).Debug("Successfully listed tags")

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

	log.With("repository", repo).Debug("Listing repository tags")

	client, err := s.backends.GetClient(repo)
	if err != nil {
		log.With("repository", repo).Errorf("Failed to get backend client for repository list: %s", err)
		return handler.Errorf("backend manager: %s", err)
	}

	opts, err := buildPaginationOptions(r.URL)
	if err != nil {
		return err
	}

	result, err := client.List(path.Join(repo, "_manifests/tags"), opts...)
	if err != nil {
		log.With("repository", repo).Errorf("Failed to list repository tags from backend: %s", err)
		return handler.Errorf("error listing from backend: %s", err)
	}

	var tags []string
	for _, name := range result.Names {
		// Strip repo prefix.
		parts := strings.Split(name, ":")
		if len(parts) != 2 {
			log.With("repository", repo, "name", name).Warn("Skipping invalid tag name format")
			continue
		}
		tags = append(tags, parts[1])
	}

	log.With("repository", repo, "tag_count", len(tags), "continuation_token", result.ContinuationToken).Debug("Successfully listed repository tags")

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

	log.With("tag", tag).Info("Received replicate tag request")

	d, err := s.store.Get(tag)
	if err != nil {
		if err == tagstore.ErrTagNotFound {
			log.With("tag", tag).Warn("Cannot replicate tag - not found in storage")
			return handler.ErrorStatus(http.StatusNotFound)
		}
		log.With("tag", tag).Errorf("Failed to get tag for replication: %s", err)
		return handler.Errorf("storage: %s", err)
	}

	log.With("tag", tag, "digest", d.String()).Debug("Retrieved tag for replication")

	deps, err := s.depResolver.Resolve(tag, d)
	if err != nil {
		log.With("tag", tag, "digest", d.String()).Errorf("Failed to resolve dependencies for replication: %s", err)
		return fmt.Errorf("resolve dependencies: %s", err)
	}

	log.With("tag", tag, "digest", d.String(), "dependency_count", len(deps)).Debug("Resolved dependencies for replication")

	if err := s.replicateTag(tag, d, deps); err != nil {
		log.With("tag", tag, "digest", d.String()).Errorf("Failed to replicate tag: %s", err)
		return err
	}

	log.With("tag", tag, "digest", d.String()).Info("Successfully initiated tag replication")

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

	log.With("tag", tag, "digest", d.String(), "delay", req.Delay, "dependency_count", len(req.Dependencies)).Debug("Received duplicate replicate request from neighbor")

	destinations := s.remotes.Match(tag)

	log.With("tag", tag, "digest", d.String(), "destination_count", len(destinations)).Debug("Matched remote destinations for duplicate replicate")

	for _, dest := range destinations {
		task := tagreplication.NewTask(tag, d, req.Dependencies, dest, req.Delay)
		if err := s.tagReplicationManager.Add(task); err != nil {
			log.With("tag", tag, "digest", d.String(), "destination", dest, "delay", req.Delay).Errorf("Failed to add replicate task from duplicate: %s", err)
			return handler.Errorf("add replicate task: %s", err)
		}
		log.With("tag", tag, "digest", d.String(), "destination", dest).Debug("Added replicate task from duplicate")
	}

	log.With("tag", tag, "digest", d.String(), "tasks_added", len(destinations)).Info("Successfully processed duplicate replicate request")

	return nil
}

func (s *Server) getOriginHandler(w http.ResponseWriter, r *http.Request) error {
	if _, err := io.WriteString(w, s.localOriginDNS); err != nil {
		return handler.Errorf("write local origin dns: %s", err)
	}
	return nil
}

func (s *Server) putTag(tag string, d core.Digest, deps core.DigestList) error {
	log.With("tag", tag, "digest", d.String(), "dependency_count", len(deps)).Debug("Validating tag dependencies")

	for i, dep := range deps {
		if _, err := s.localOriginClient.Stat(tag, dep); err == blobclient.ErrBlobNotFound {
			log.With("tag", tag, "digest", d.String(), "missing_dependency", dep.String(), "dependency_index", i).Error("Missing dependency blob")
			return handler.Errorf("cannot upload tag, missing dependency %s", dep)
		} else if err != nil {
			log.With("tag", tag, "digest", d.String(), "dependency", dep.String(), "dependency_index", i).Errorf("Failed to check dependency blob: %s", err)
			return handler.Errorf("check blob: %s", err)
		}
	}

	log.With("tag", tag, "digest", d.String()).Debug("All dependencies validated successfully")

	if err := s.store.Put(tag, d, 0); err != nil {
		log.With("tag", tag, "digest", d.String()).Errorf("Failed to store tag: %s", err)
		return handler.Errorf("storage: %s", err)
	}

	log.With("tag", tag, "digest", d.String()).Info("Tag stored locally")

	neighbors := s.neighbors.Resolve()
	neighborCount := len(neighbors)

	log.With("tag", tag, "digest", d.String(), "neighbor_count", neighborCount).Debug("Starting neighbor replication")

	var delay time.Duration
	var successes int
	for addr := range neighbors {
		delay += s.config.DuplicatePutStagger
		client := s.provider.Provide(addr)
		if err := client.DuplicatePut(tag, d, delay); err != nil {
			log.With("tag", tag, "digest", d.String(), "neighbor", addr, "delay", delay).Errorf("Failed to duplicate put to neighbor: %s", err)
		} else {
			successes++
			log.With("tag", tag, "digest", d.String(), "neighbor", addr, "delay", delay).Debug("Successfully duplicated put to neighbor")
		}
	}

	log.With("tag", tag, "digest", d.String(), "total_neighbors", neighborCount, "successful_neighbors", successes, "failed_neighbors", neighborCount-successes).Info("Completed neighbor replication")

	if len(neighbors) != 0 && successes == 0 {
		s.stats.Counter("duplicate_put_failures").Inc(1)
		log.With("tag", tag, "digest", d.String(), "neighbor_count", neighborCount).Error("All neighbor replications failed")
	}
	return nil
}

func (s *Server) replicateTag(tag string, d core.Digest, deps core.DigestList) error {
	destinations := s.remotes.Match(tag)

	log.With("tag", tag, "digest", d.String(), "destination_count", len(destinations)).Debug("Checking remote destinations for tag replication")

	if len(destinations) == 0 {
		log.With("tag", tag, "digest", d.String()).Debug("No remote destinations configured for tag")
		return nil
	}

	log.With("tag", tag, "digest", d.String(), "destinations", destinations).Info("Adding remote replication tasks")

	for _, dest := range destinations {
		task := tagreplication.NewTask(tag, d, deps, dest, 0)
		if err := s.tagReplicationManager.Add(task); err != nil {
			log.With("tag", tag, "digest", d.String(), "destination", dest).Errorf("Failed to add remote replication task: %s", err)
			return handler.Errorf("add replicate task: %s", err)
		}
		log.With("tag", tag, "digest", d.String(), "destination", dest).Debug("Added remote replication task")
	}

	neighbors := s.neighbors.Resolve()
	neighborCount := len(neighbors)

	log.With("tag", tag, "digest", d.String(), "neighbor_count", neighborCount).Debug("Notifying neighbors about remote replication")

	var delay time.Duration
	var successes int
	for addr := range neighbors { // Loops in random order.
		delay += s.config.DuplicateReplicateStagger
		client := s.provider.Provide(addr)
		if err := client.DuplicateReplicate(tag, d, deps, delay); err != nil {
			log.With("tag", tag, "digest", d.String(), "neighbor", addr, "delay", delay).Errorf("Failed to notify neighbor about replication: %s", err)
		} else {
			successes++
			log.With("tag", tag, "digest", d.String(), "neighbor", addr, "delay", delay).Debug("Successfully notified neighbor about replication")
		}
	}

	log.With("tag", tag, "digest", d.String(), "remote_destinations", len(destinations), "notified_neighbors", successes, "failed_neighbors", neighborCount-successes).Info("Completed remote replication setup")

	if len(neighbors) != 0 && successes == 0 {
		s.stats.Counter("duplicate_replicate_failures").Inc(1)
		log.With("tag", tag, "digest", d.String(), "neighbor_count", neighborCount).Error("All neighbor replication notifications failed")
	}
	return nil
}

// digestListToStrings converts a DigestList to a slice of strings for logging
func digestListToStrings(digests core.DigestList) []string {
	result := make([]string, len(digests))
	for i, d := range digests {
		result[i] = d.String()
	}
	return result
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
	result []string,
) (*tagmodels.ListResponse, error) {
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
