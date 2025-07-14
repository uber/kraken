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
package blobserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/blobrefresh"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/lib/metainfogen"
	"github.com/uber/kraken/lib/middleware"
	"github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/lib/persistedretry/writeback"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/errutil"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/listener"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/memsize"
	"github.com/uber/kraken/utils/stringset"

	"github.com/andres-erbsen/clock"
	"github.com/go-chi/chi"
	"github.com/uber-go/tally"
)

const _uploadChunkSize = 16 * memsize.MB

// Server defines a server that serves blob data for agent.
type Server struct {
	config            Config
	stats             tally.Scope
	clk               clock.Clock
	addr              string
	hashRing          hashring.Ring
	cas               *store.CAStore
	clientProvider    blobclient.Provider
	clusterProvider   blobclient.ClusterProvider
	backends          *backend.Manager
	blobRefresher     *blobrefresh.Refresher
	metaInfoGenerator *metainfogen.Generator
	uploader          *uploader
	writeBackManager  persistedretry.Manager

	// This is an unfortunate coupling between the p2p client and the blob server.
	// Tracker queries the origin cluster to discover which origins can seed
	// a given torrent, however this requires blob server to understand the
	// context of the p2p client running alongside it.
	pctx core.PeerContext
	
	// Resource management
	downloadSemaphore chan struct{}
	uploadSemaphore   chan struct{}
	
	// Metrics
	downloadTimer        tally.Timer
	uploadTimer          tally.Timer
	replicationTimer     tally.Timer
	downloadCounter      tally.Counter
	uploadCounter        tally.Counter
	replicationCounter   tally.Counter
	errorCounter         tally.Counter
	timeoutCounter       tally.Counter
	resourceLeakCounter  tally.Counter
}

// New initializes a new Server.
func New(
	config Config,
	stats tally.Scope,
	clk clock.Clock,
	addr string,
	hashRing hashring.Ring,
	cas *store.CAStore,
	clientProvider blobclient.Provider,
	clusterProvider blobclient.ClusterProvider,
	pctx core.PeerContext,
	backends *backend.Manager,
	blobRefresher *blobrefresh.Refresher,
	metaInfoGenerator *metainfogen.Generator,
	writeBackManager persistedretry.Manager) (*Server, error) {

	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "blobserver",
	})

	return &Server{
		config:              config,
		stats:               stats,
		clk:                 clk,
		addr:                addr,
		hashRing:            hashRing,
		cas:                 cas,
		clientProvider:      clientProvider,
		clusterProvider:     clusterProvider,
		backends:            backends,
		blobRefresher:       blobRefresher,
		metaInfoGenerator:   metaInfoGenerator,
		uploader:            newUploader(cas),
		writeBackManager:    writeBackManager,
		pctx:                pctx,
		downloadSemaphore:   make(chan struct{}, config.MaxConcurrentDownloads),
		uploadSemaphore:     make(chan struct{}, config.MaxConcurrentUploads),
		downloadTimer:       stats.Timer("download_duration"),
		uploadTimer:         stats.Timer("upload_duration"),
		replicationTimer:    stats.Timer("replication_duration"),
		downloadCounter:     stats.Counter("downloads"),
		uploadCounter:       stats.Counter("uploads"),
		replicationCounter:  stats.Counter("replications"),
		errorCounter:        stats.Counter("errors"),
		timeoutCounter:      stats.Counter("timeouts"),
		resourceLeakCounter: stats.Counter("resource_leaks"),
	}, nil
}

// Addr returns the address the blob server is configured on.
func (s *Server) Addr() string {
	return s.addr
}

// Handler returns an http handler for the blob server.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))
	r.Use(s.requestTracingMiddleware)
	r.Use(s.requestValidationMiddleware)

	// Public endpoints:

	r.Get("/health", handler.Wrap(s.healthCheckHandler))
	r.Get("/readiness", handler.Wrap(s.readinessCheckHandler))

	r.Get("/blobs/{digest}/locations", handler.Wrap(s.getLocationsHandler))

	r.Post("/namespace/{namespace}/blobs/{digest}/uploads", handler.Wrap(s.startClusterUploadHandler))
	r.Patch("/namespace/{namespace}/blobs/{digest}/uploads/{uid}", handler.Wrap(s.patchClusterUploadHandler))
	r.Put("/namespace/{namespace}/blobs/{digest}/uploads/{uid}", handler.Wrap(s.commitClusterUploadHandler))

	r.Get("/namespace/{namespace}/blobs/{digest}", handler.Wrap(s.downloadBlobHandler))

	r.Post("/namespace/{namespace}/blobs/{digest}/remote/{remote}", handler.Wrap(s.replicateToRemoteHandler))

	r.Post("/forcecleanup", handler.Wrap(s.forceCleanupHandler))

	// Internal endpoints:

	r.Post("/internal/blobs/{digest}/uploads", handler.Wrap(s.startTransferHandler))
	r.Patch("/internal/blobs/{digest}/uploads/{uid}", handler.Wrap(s.patchTransferHandler))
	r.Put("/internal/blobs/{digest}/uploads/{uid}", handler.Wrap(s.commitTransferHandler))

	r.Delete("/internal/blobs/{digest}", handler.Wrap(s.deleteBlobHandler))

	r.Post("/internal/blobs/{digest}/metainfo", handler.Wrap(s.overwriteMetaInfoHandler))

	r.Get("/internal/peercontext", handler.Wrap(s.getPeerContextHandler))

	r.Head("/internal/namespace/{namespace}/blobs/{digest}", handler.Wrap(s.statHandler))

	r.Get("/internal/namespace/{namespace}/blobs/{digest}/metainfo", handler.Wrap(s.getMetaInfoHandler))

	r.Put(
		"/internal/duplicate/namespace/{namespace}/blobs/{digest}/uploads/{uid}",
		handler.Wrap(s.duplicateCommitClusterUploadHandler))

	r.Mount("/", http.DefaultServeMux) // Serves /debug/pprof endpoints.

	return r
}

// requestTracingMiddleware adds structured logging with request tracing
func (s *Server) requestTracingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := fmt.Sprintf("%d", rand.Int63())
		start := time.Now()
		
		// Add request ID to context for downstream handlers
		ctx := context.WithValue(r.Context(), "request_id", requestID)
		r = r.WithContext(ctx)
		
		// Add request ID to response headers for debugging
		w.Header().Set("X-Request-ID", requestID)
		
		log.With(
			"request_id", requestID,
			"method", r.Method,
			"path", r.URL.Path,
			"remote_addr", r.RemoteAddr,
		).Info("Request started")
		
		defer func() {
			duration := time.Since(start)
			log.With(
				"request_id", requestID,
				"method", r.Method,
				"path", r.URL.Path,
				"duration_ms", duration.Milliseconds(),
			).Info("Request completed")
		}()
		
		next.ServeHTTP(w, r)
	})
}

// requestValidationMiddleware validates request size and other basic requirements
func (s *Server) requestValidationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > s.config.MaxRequestSize {
			http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ListenAndServe is a blocking call which runs s.
func (s *Server) ListenAndServe(h http.Handler) error {
	log.Infof("Starting blob server on %s", s.config.Listener)
	return listener.Serve(s.config.Listener, h)
}

func (s *Server) healthCheckHandler(w http.ResponseWriter, r *http.Request) error {
	fmt.Fprintln(w, "OK")
	return nil
}

func (s *Server) readinessCheckHandler(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), s.config.ReadinessTimeout)
	defer cancel()
	
	requestID := s.getRequestID(r)
	logger := log.With("request_id", requestID, "operation", "readiness_check")
	
	done := make(chan error, 1)
	go func() {
		done <- s.backends.CheckReadiness()
	}()
	
	select {
	case err := <-done:
		if err != nil {
			logger.Errorf("Readiness check failed: %s", err)
			return handler.Errorf("not ready to serve traffic: %s", err).Status(http.StatusServiceUnavailable)
		}
		logger.Info("Readiness check passed")
		fmt.Fprintln(w, "OK")
		return nil
	case <-ctx.Done():
		s.timeoutCounter.Inc(1)
		logger.Error("Readiness check timed out")
		return handler.Errorf("readiness check timed out").Status(http.StatusServiceUnavailable)
	}
}

// statHandler returns blob info if it exists.
func (s *Server) statHandler(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), s.config.BackendTimeout)
	defer cancel()
	
	requestID := s.getRequestID(r)
	logger := log.With("request_id", requestID, "operation", "stat")
	
	checkLocal, err := strconv.ParseBool(httputil.GetQueryArg(r, "local", "false"))
	if err != nil {
		logger.Errorf("Failed to parse local parameter: %s", err)
		return handler.Errorf("parse arg `local` as bool: %s", err)
	}
	
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		logger.Errorf("Failed to parse namespace parameter: %s", err)
		return err
	}
	
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		logger.Errorf("Failed to parse digest parameter: %s", err)
		return err
	}

	logger = logger.With("namespace", namespace, "digest", d.Hex(), "local", checkLocal)
	logger.Info("Starting blob stat")

	bi, err := s.stat(ctx, namespace, d, checkLocal)
	if os.IsNotExist(err) {
		logger.Info("Blob not found")
		return handler.ErrorStatus(http.StatusNotFound)
	} else if err != nil {
		logger.Errorf("Blob stat failed: %s", err)
		return fmt.Errorf("stat: %s", err)
	}
	
	w.Header().Set("Content-Length", strconv.FormatInt(bi.Size, 10))
	logger.With("size", bi.Size).Info("Blob stat completed successfully")
	return nil
}

func (s *Server) stat(ctx context.Context, namespace string, d core.Digest, checkLocal bool) (*core.BlobInfo, error) {
	fi, err := s.cas.GetCacheFileStat(d.Hex())
	if err == nil {
		return core.NewBlobInfo(fi.Size()), nil
	} else if os.IsNotExist(err) {
		if !checkLocal {
			client, err := s.backends.GetClient(namespace)
			if err != nil {
				return nil, fmt.Errorf("get backend client: %s", err)
			}
			
			done := make(chan struct {
				bi *core.BlobInfo
				err error
			}, 1)
			
			go func() {
				bi, err := client.Stat(namespace, d.Hex())
				done <- struct {
					bi *core.BlobInfo
					err error
				}{bi, err}
			}()
			
			select {
			case result := <-done:
				if result.err == nil {
					return result.bi, nil
				} else if result.err == backenderrors.ErrBlobNotFound {
					return nil, os.ErrNotExist
				} else {
					return nil, fmt.Errorf("backend stat: %s", result.err)
				}
			case <-ctx.Done():
				s.timeoutCounter.Inc(1)
				return nil, fmt.Errorf("backend stat timed out: %s", ctx.Err())
			}
		}
		return nil, err // os.ErrNotExist
	}

	return nil, fmt.Errorf("stat cache file: %s", err)
}

func (s *Server) downloadBlobHandler(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), s.config.DownloadTimeout)
	defer cancel()
	
	requestID := s.getRequestID(r)
	logger := log.With("request_id", requestID, "operation", "download")
	
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		logger.Errorf("Failed to parse namespace parameter: %s", err)
		return err
	}
	
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		logger.Errorf("Failed to parse digest parameter: %s", err)
		return err
	}
	
	logger = logger.With("namespace", namespace, "digest", d.Hex())
	logger.Info("Starting blob download")
	
	// Acquire download semaphore
	select {
	case s.downloadSemaphore <- struct{}{}:
		defer func() { <-s.downloadSemaphore }()
	case <-ctx.Done():
		logger.Error("Download semaphore acquisition timed out")
		return handler.Errorf("download queue full").Status(http.StatusServiceUnavailable)
	}
	
	s.downloadCounter.Inc(1)
	timer := s.downloadTimer.Start()
	defer timer.Stop()
	
	if err := s.downloadBlob(ctx, namespace, d, w); err != nil {
		s.errorCounter.Inc(1)
		logger.Errorf("Download failed: %s", err)
		return err
	}
	
	setOctetStreamContentType(w)
	logger.Info("Download completed successfully")
	return nil
}

func (s *Server) replicateToRemoteHandler(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), s.config.ReplicationTimeout)
	defer cancel()
	
	requestID := s.getRequestID(r)
	logger := log.With("request_id", requestID, "operation", "replicate_to_remote")
	
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		logger.Errorf("Failed to parse namespace parameter: %s", err)
		return err
	}
	
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		logger.Errorf("Failed to parse digest parameter: %s", err)
		return err
	}
	
	remote, err := httputil.ParseParam(r, "remote")
	if err != nil {
		logger.Errorf("Failed to parse remote parameter: %s", err)
		return err
	}
	
	logger = logger.With("namespace", namespace, "digest", d.Hex(), "remote", remote)
	logger.Info("Starting remote replication")
	
	s.replicationCounter.Inc(1)
	timer := s.replicationTimer.Start()
	defer timer.Stop()
	
	if err := s.replicateToRemote(ctx, namespace, d, remote); err != nil {
		s.errorCounter.Inc(1)
		logger.Errorf("Remote replication failed: %s", err)
		return err
	}
	
	logger.Info("Remote replication completed successfully")
	return nil
}

func (s *Server) replicateToRemote(ctx context.Context, namespace string, d core.Digest, remoteDNS string) error {
	f, err := s.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		if os.IsNotExist(err) {
			return s.startRemoteBlobDownload(namespace, d, false)
		}
		return handler.Errorf("file store: %s", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			s.resourceLeakCounter.Inc(1)
			log.Errorf("Failed to close file reader: %s", closeErr)
		}
	}()

	remote, err := s.clusterProvider.Provide(remoteDNS)
	if err != nil {
		return handler.Errorf("remote cluster provider: %s", err)
	}
	
	done := make(chan error, 1)
	go func() {
		done <- remote.UploadBlob(namespace, d, f)
	}()
	
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		s.timeoutCounter.Inc(1)
		return handler.Errorf("remote replication timed out: %s", ctx.Err())
	}
}

// deleteBlobHandler deletes blob data.
func (s *Server) deleteBlobHandler(w http.ResponseWriter, r *http.Request) error {
	requestID := s.getRequestID(r)
	logger := log.With("request_id", requestID, "operation", "delete")
	
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		logger.Errorf("Failed to parse digest parameter: %s", err)
		return err
	}
	
	logger = logger.With("digest", d.Hex())
	logger.Info("Starting blob deletion")
	
	if err := s.deleteBlob(d); err != nil {
		s.errorCounter.Inc(1)
		logger.Errorf("Blob deletion failed: %s", err)
		return err
	}
	
	setContentLength(w, 0)
	w.WriteHeader(http.StatusAccepted)
	logger.Info("Blob deletion completed successfully")
	return nil
}

func (s *Server) getLocationsHandler(w http.ResponseWriter, r *http.Request) error {
	requestID := s.getRequestID(r)
	logger := log.With("request_id", requestID, "operation", "get_locations")
	
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		logger.Errorf("Failed to parse digest parameter: %s", err)
		return err
	}
	
	logger = logger.With("digest", d.Hex())
	logger.Info("Getting blob locations")
	
	locs := s.hashRing.Locations(d)
	w.Header().Set("Origin-Locations", strings.Join(locs, ","))
	w.WriteHeader(http.StatusOK)
	
	logger.With("locations", locs).Info("Blob locations retrieved successfully")
	return nil
}

// getPeerContextHandler returns the Server's peer context as JSON.
func (s *Server) getPeerContextHandler(w http.ResponseWriter, r *http.Request) error {
	requestID := s.getRequestID(r)
	logger := log.With("request_id", requestID, "operation", "get_peer_context")
	
	logger.Info("Getting peer context")
	
	if err := json.NewEncoder(w).Encode(s.pctx); err != nil {
		s.errorCounter.Inc(1)
		logger.Errorf("Failed to encode peer context: %s", err)
		return handler.Errorf("error converting peer context to json: %s", err)
	}
	
	logger.Info("Peer context retrieved successfully")
	return nil
}

func (s *Server) getMetaInfoHandler(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), s.config.BackendTimeout)
	defer cancel()
	
	requestID := s.getRequestID(r)
	logger := log.With("request_id", requestID, "operation", "get_metainfo")
	
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		logger.Errorf("Failed to parse namespace parameter: %s", err)
		return err
	}
	
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		logger.Errorf("Failed to parse digest parameter: %s", err)
		return err
	}
	
	logger = logger.With("namespace", namespace, "digest", d.Hex())
	logger.Info("Getting metainfo")
	
	raw, err := s.getMetaInfo(ctx, namespace, d)
	if err != nil {
		s.errorCounter.Inc(1)
		logger.Errorf("Failed to get metainfo: %s", err)
		return err
	}
	
	w.Write(raw)
	logger.Info("Metainfo retrieved successfully")
	return nil
}

func (s *Server) overwriteMetaInfoHandler(w http.ResponseWriter, r *http.Request) error {
	requestID := s.getRequestID(r)
	logger := log.With("request_id", requestID, "operation", "overwrite_metainfo")
	
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		logger.Errorf("Failed to parse digest parameter: %s", err)
		return err
	}
	
	pieceLength, err := strconv.ParseInt(r.URL.Query().Get("piece_length"), 10, 64)
	if err != nil {
		logger.Errorf("Failed to parse piece_length parameter: %s", err)
		return handler.Errorf("invalid piece_length argument: %s", err).Status(http.StatusBadRequest)
	}
	
	logger = logger.With("digest", d.Hex(), "piece_length", pieceLength)
	logger.Info("Overwriting metainfo")
	
	if err := s.overwriteMetaInfo(d, pieceLength); err != nil {
		s.errorCounter.Inc(1)
		logger.Errorf("Failed to overwrite metainfo: %s", err)
		return err
	}
	
	logger.Info("Metainfo overwritten successfully")
	return nil
}

// overwriteMetaInfo generates metainfo configured with pieceLength for d and
// writes it to disk, overwriting any existing metainfo. Primarily intended for
// benchmarking purposes.
func (s *Server) overwriteMetaInfo(d core.Digest, pieceLength int64) error {
	f, err := s.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		return handler.Errorf("get cache file: %s", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			s.resourceLeakCounter.Inc(1)
			log.Errorf("Failed to close file reader in overwriteMetaInfo: %s", closeErr)
		}
	}()
	
	mi, err := core.NewMetaInfo(d, f, pieceLength)
	if err != nil {
		return handler.Errorf("create metainfo: %s", err)
	}
	
	if _, err := s.cas.SetCacheFileMetadata(d.Hex(), metadata.NewTorrentMeta(mi)); err != nil {
		return handler.Errorf("set metainfo: %s", err)
	}
	
	return nil
}

// getMetaInfo returns metainfo for d. If no blob exists under d, a download of
// the blob from the storage backend configured for namespace will be initiated.
// This download is asynchronous and getMetaInfo will immediately return a
// "202 Accepted" server error.
func (s *Server) getMetaInfo(ctx context.Context, namespace string, d core.Digest) ([]byte, error) {
	var tm metadata.TorrentMeta
	if err := s.cas.GetCacheFileMetadata(d.Hex(), &tm); os.IsNotExist(err) {
		return nil, s.startRemoteBlobDownload(namespace, d, true)
	} else if err != nil {
		return nil, handler.Errorf("get cache metadata: %s", err)
	}
	return tm.Serialize()
}

type localReplicationHook struct {
	server *Server
}

func (h *localReplicationHook) Run(d core.Digest) {
	timer := h.server.stats.Timer("replicate_blob").Start()
	defer timer.Stop()
	
	if err := h.server.replicateBlobLocally(d); err != nil {
		// Don't return error here as we only want to cache storage backend errors.
		log.With("blob", d.Hex()).Errorf("Error replicating remote blob: %s", err)
		h.server.stats.Counter("replicate_blob_errors").Inc(1)
		return
	}
}

func (s *Server) startRemoteBlobDownload(
	namespace string, d core.Digest, replicateLocally bool) error {

	var hooks []blobrefresh.PostHook
	if replicateLocally {
		hooks = append(hooks, &localReplicationHook{s})
	}
	err := s.blobRefresher.Refresh(namespace, d, hooks...)
	switch err {
	case blobrefresh.ErrPending, nil:
		return handler.ErrorStatus(http.StatusAccepted)
	case blobrefresh.ErrNotFound:
		return handler.ErrorStatus(http.StatusNotFound)
	case blobrefresh.ErrWorkersBusy:
		return handler.ErrorStatus(http.StatusServiceUnavailable)
	default:
		return err
	}
}

func (s *Server) replicateBlobLocally(d core.Digest) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReplicationTimeout)
	defer cancel()
	
	return s.applyToReplicas(ctx, d, func(i int, client blobclient.Client) error {
		f, err := s.cas.GetCacheFileReader(d.Hex())
		if err != nil {
			return fmt.Errorf("get cache reader: %s", err)
		}
		defer func() {
			if closeErr := f.Close(); closeErr != nil {
				s.resourceLeakCounter.Inc(1)
				log.Errorf("Failed to close file reader in replicateBlobLocally: %s", closeErr)
			}
		}()
		
		done := make(chan error, 1)
		go func() {
			done <- client.TransferBlob(d, f)
		}()
		
		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("transfer blob: %s", err)
			}
			return nil
		case <-ctx.Done():
			s.timeoutCounter.Inc(1)
			return fmt.Errorf("transfer blob timed out: %s", ctx.Err())
		}
	})
}

// applyToReplicas applies f to the replicas of d concurrently in random order,
// not including the current origin. Passes the index of the iteration to f.
func (s *Server) applyToReplicas(ctx context.Context, d core.Digest, f func(i int, c blobclient.Client) error) error {
	replicas := stringset.FromSlice(s.hashRing.Locations(d))
	replicas.Remove(s.addr)

	var mu sync.Mutex
	var errs []error

	var wg sync.WaitGroup
	var i int
	for replica := range replicas {
		wg.Add(1)
		go func(i int, replica string) {
			defer wg.Done()
			if err := f(i, s.clientProvider.Provide(replica)); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(i, replica)
		i++
	}
	
	// Wait for all goroutines to complete or context to be cancelled
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		return errutil.Join(errs)
	case <-ctx.Done():
		s.timeoutCounter.Inc(1)
		return fmt.Errorf("replicas operation timed out: %s", ctx.Err())
	}
}

// downloadBlob downloads blob for d into dst. If no blob exists under d, a
// download of the blob from the storage backend configured for namespace will
// be initiated. This download is asynchronous and downloadBlob will immediately
// return a "202 Accepted" handler error.
func (s *Server) downloadBlob(ctx context.Context, namespace string, d core.Digest, dst io.Writer) error {
	f, err := s.cas.GetCacheFileReader(d.Hex())
	if os.IsNotExist(err) {
		return s.startRemoteBlobDownload(namespace, d, true)
	} else if err != nil {
		return handler.Errorf("get cache file: %s", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			s.resourceLeakCounter.Inc(1)
			log.Errorf("Failed to close file reader in downloadBlob: %s", closeErr)
		}
	}()

	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(dst, f)
		done <- err
	}()
	
	select {
	case err := <-done:
		if err != nil {
			return handler.Errorf("copy blob: %s", err)
		}
		return nil
	case <-ctx.Done():
		s.timeoutCounter.Inc(1)
		return handler.Errorf("download blob timed out: %s", ctx.Err())
	}
}

func (s *Server) deleteBlob(d core.Digest) error {
	if err := s.cas.DeleteCacheFile(d.Hex()); err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("cannot delete blob data for digest %q: %s", d, err)
	}
	return nil
}

// startTransferHandler initializes an upload for internal blob transfers.
func (s *Server) startTransferHandler(w http.ResponseWriter, r *http.Request) error {
	requestID := s.getRequestID(r)
	logger := log.With("request_id", requestID, "operation", "start_transfer")
	
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		logger.Errorf("Failed to parse digest parameter: %s", err)
		return err
	}
	
	logger = logger.With("digest", d.Hex())
	logger.Info("Starting internal transfer")
	
	if ok, err := blobExists(s.cas, d); err != nil {
		s.errorCounter.Inc(1)
		logger.Errorf("Failed to check blob existence: %s", err)
		return handler.Errorf("check blob: %s", err)
	} else if ok {
		logger.Info("Blob already exists")
		return handler.ErrorStatus(http.StatusConflict)
	}
	
	uid, err := s.uploader.start(d)
	if err != nil {
		s.errorCounter.Inc(1)
		logger.Errorf("Failed to start upload: %s", err)
		return err
	}
	
	setUploadLocation(w, uid)
	w.WriteHeader(http.StatusOK)
	logger.With("upload_id", uid).Info("Internal transfer started successfully")
	return nil
}

// patchTransferHandler uploads a chunk of a blob for internal uploads.
func (s *Server) patchTransferHandler(w http.ResponseWriter, r *http.Request) error {
	requestID := s.getRequestID(r)
	logger := log.With("request_id", requestID, "operation", "patch_transfer")
	
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		logger.Errorf("Failed to parse digest parameter: %s", err)
		return err
	}
	
	uid, err := httputil.ParseParam(r, "uid")
	if err != nil {
		logger.Errorf("Failed to parse uid parameter: %s", err)
		return err
	}
	
	start, end, err := parseContentRange(r.Header)
	if err != nil {
		logger.Errorf("Failed to parse content range: %s", err)
		return err
	}
	
	logger = logger.With("digest", d.Hex(), "upload_id", uid, "start", start, "end", end)
	logger.Info("Patching internal transfer")
	
	if err := s.uploader.patch(d, uid, r.Body, start, end); err != nil {
		s.errorCounter.Inc(1)
		logger.Errorf("Failed to patch transfer: %s", err)
		return err
	}
	
	logger.Info("Internal transfer patched successfully")
	return nil
}

// commitTransferHandler commits the upload of an internal blob transfer.
// Internal blob transfers are not replicated to the rest of the cluster.
func (s *Server) commitTransferHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	uid, err := httputil.ParseParam(r, "uid")
	if err != nil {
		return err
	}
	if err := s.uploader.commit(d, uid); err != nil {
		return err
	}
	if err := s.metaInfoGenerator.Generate(d); err != nil {
		return handler.Errorf("generate metainfo: %s", err)
	}
	return nil
}

func (s *Server) handleUploadConflict(err error, namespace string, d core.Digest) error {
	if herr, ok := err.(*handler.Error); ok && herr.GetStatus() == http.StatusConflict {
		// Even if the blob was already uploaded and committed to cache, it's
		// still possible that adding the write-back task failed. Clients short
		// circuit on conflict and return success, so we must make sure that if we
		// tell a client to stop before commit, the blob has been written back.
		if err := s.writeBack(namespace, d, 0); err != nil {
			return err
		}
	}
	return err
}

// startClusterUploadHandler initializes an upload for external uploads.
func (s *Server) startClusterUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	uid, err := s.uploader.start(d)
	if err != nil {
		return s.handleUploadConflict(err, namespace, d)
	}
	setUploadLocation(w, uid)
	w.WriteHeader(http.StatusOK)
	return nil
}

// patchClusterUploadHandler uploads a chunk of a blob for external uploads.
func (s *Server) patchClusterUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	uid, err := httputil.ParseParam(r, "uid")
	if err != nil {
		return err
	}
	start, end, err := parseContentRange(r.Header)
	if err != nil {
		return err
	}
	if err := s.uploader.patch(d, uid, r.Body, start, end); err != nil {
		return s.handleUploadConflict(err, namespace, d)
	}
	return nil
}

// commitClusterUploadHandler commits an external blob upload asynchronously,
// meaning the blob will be written back to remote storage in a non-blocking
// fashion.
func (s *Server) commitClusterUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	uid, err := httputil.ParseParam(r, "uid")
	if err != nil {
		return err
	}

	if err := s.uploader.commit(d, uid); err != nil {
		return s.handleUploadConflict(err, namespace, d)
	}
	if err := s.writeBack(namespace, d, 0); err != nil {
		return err
	}
	err = s.applyToReplicas(d, func(i int, client blobclient.Client) error {
		delay := s.config.DuplicateWriteBackStagger * time.Duration(i+1)
		f, err := s.cas.GetCacheFileReader(d.Hex())
		if err != nil {
			return fmt.Errorf("get cache file: %s", err)
		}
		if err := client.DuplicateUploadBlob(namespace, d, f, delay); err != nil {
			return fmt.Errorf("duplicate upload: %s", err)
		}
		return nil
	})
	if err != nil {
		s.stats.Counter("duplicate_write_back_errors").Inc(1)
		log.Errorf("Error duplicating write-back task to replicas: %s", err)
	}
	return nil
}

// duplicateCommitClusterUploadHandler commits a duplicate blob upload, which
// will attempt to write-back after the requested delay.
func (s *Server) duplicateCommitClusterUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	uid, err := httputil.ParseParam(r, "uid")
	if err != nil {
		return err
	}

	var dr blobclient.DuplicateCommitUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&dr); err != nil {
		return handler.Errorf("decode body: %s", err)
	}
	delay := dr.Delay

	if err := s.uploader.commit(d, uid); err != nil {
		return err
	}
	return s.writeBack(namespace, d, delay)
}

func (s *Server) writeBack(namespace string, d core.Digest, delay time.Duration) error {
	if _, err := s.cas.SetCacheFileMetadata(d.Hex(), metadata.NewPersist(true)); err != nil {
		return handler.Errorf("set persist metadata: %s", err)
	}
	task := writeback.NewTask(namespace, d.Hex(), delay)
	if err := s.writeBackManager.Add(task); err != nil {
		return handler.Errorf("add write-back task: %s", err)
	}
	if err := s.metaInfoGenerator.Generate(d); err != nil {
		return handler.Errorf("generate metainfo: %s", err)
	}
	return nil
}

func (s *Server) forceCleanupHandler(w http.ResponseWriter, r *http.Request) error {
	// Note, this API is intended to be executed manually (i.e. curl), hence the
	// query arguments, usage of hours instead of nanoseconds, and JSON response
	// enumerating deleted files / errors.

	rawTTLHr := r.URL.Query().Get("ttl_hr")
	if rawTTLHr == "" {
		return handler.Errorf("query arg ttl_hr required").Status(http.StatusBadRequest)
	}
	ttlHr, err := strconv.Atoi(rawTTLHr)
	if err != nil {
		return handler.Errorf("invalid ttl_hr: %s", err).Status(http.StatusBadRequest)
	}
	ttl := time.Duration(ttlHr) * time.Hour

	names, err := s.cas.ListCacheFiles()
	if err != nil {
		return err
	}
	var errs, deleted []string
	for _, name := range names {
		if ok, err := s.maybeDelete(name, ttl); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %s", name, err))
		} else if ok {
			deleted = append(deleted, name)
		}
	}
	return json.NewEncoder(w).Encode(map[string]interface{}{
		"deleted": deleted,
		"errors":  errs,
	})
}

func (s *Server) maybeDelete(name string, ttl time.Duration) (deleted bool, err error) {
	d, err := core.NewSHA256DigestFromHex(name)
	if err != nil {
		return false, fmt.Errorf("parse digest: %s", err)
	}
	info, err := s.cas.GetCacheFileStat(name)
	if err != nil {
		return false, fmt.Errorf("store: %s", err)
	}
	expired := s.clk.Now().Sub(info.ModTime()) > ttl
	owns := stringset.FromSlice(s.hashRing.Locations(d)).Has(s.addr)
	if expired || !owns {
		// Ensure file is backed up properly before deleting.
		var pm metadata.Persist
		if err := s.cas.GetCacheFileMetadata(name, &pm); err != nil && !os.IsNotExist(err) {
			return false, fmt.Errorf("store: %s", err)
		}
		if pm.Value {
			// Note: It is possible that no writeback tasks exist, but the file
			// is persisted. We classify this as a leaked file which is safe to
			// delete.
			tasks, err := s.writeBackManager.Find(writeback.NewNameQuery(name))
			if err != nil {
				return false, fmt.Errorf("find writeback tasks: %s", err)
			}
			for _, task := range tasks {
				if err := s.writeBackManager.SyncExec(task); err != nil {
					return false, fmt.Errorf("writeback: %s", err)
				}
			}
			if err := s.cas.DeleteCacheFileMetadata(name, &metadata.Persist{}); err != nil {
				return false, fmt.Errorf("delete persist: %s", err)
			}
		}
		if err := s.cas.DeleteCacheFile(name); err != nil {
			return false, fmt.Errorf("delete: %s", err)
		}
		return true, nil
	}
	return false, nil
}

// getRequestID extracts the request ID from the request context
func (s *Server) getRequestID(r *http.Request) string {
	if id, ok := r.Context().Value("request_id").(string); ok {
		return id
	}
	return "unknown"
}

// setOctetStreamContentType sets the content type to application/octet-stream
func setOctetStreamContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/octet-stream")
}

// setContentLength sets the content length header
func setContentLength(w http.ResponseWriter, length int) {
	w.Header().Set("Content-Length", strconv.Itoa(length))
}

// setUploadLocation sets the upload location header
func setUploadLocation(w http.ResponseWriter, uid string) {
	w.Header().Set("Location", uid)
}

// blobExists checks if a blob exists in the cache
func blobExists(cas *store.CAStore, d core.Digest) (bool, error) {
	_, err := cas.GetCacheFileStat(d.Hex())
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// parseContentRange parses the content range header
func parseContentRange(headers http.Header) (start, end int64, err error) {
	rangeHeader := headers.Get("Content-Range")
	if rangeHeader == "" {
		return 0, 0, fmt.Errorf("missing Content-Range header")
	}
	
	// Parse "bytes start-end/total" format
	parts := strings.Split(rangeHeader, " ")
	if len(parts) != 2 || parts[0] != "bytes" {
		return 0, 0, fmt.Errorf("invalid Content-Range format")
	}
	
	rangeParts := strings.Split(parts[1], "/")
	if len(rangeParts) != 2 {
		return 0, 0, fmt.Errorf("invalid Content-Range format")
	}
	
	startEndParts := strings.Split(rangeParts[0], "-")
	if len(startEndParts) != 2 {
		return 0, 0, fmt.Errorf("invalid Content-Range format")
	}
	
	start, err = strconv.ParseInt(startEndParts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start range: %s", err)
	}
	
	end, err = strconv.ParseInt(startEndParts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end range: %s", err)
	}
	
	return start, end, nil
}
