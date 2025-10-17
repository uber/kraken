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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/go-chi/chi"
	"github.com/uber-go/tally"
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
	"github.com/uber/kraken/utils/stringset"
)

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
	writeBackManager persistedretry.Manager,
) (*Server, error) {
	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "blobserver",
	})

	return &Server{
		config:            config,
		stats:             stats,
		clk:               clk,
		addr:              addr,
		hashRing:          hashRing,
		cas:               cas,
		clientProvider:    clientProvider,
		clusterProvider:   clusterProvider,
		backends:          backends,
		blobRefresher:     blobRefresher,
		metaInfoGenerator: metaInfoGenerator,
		uploader:          newUploader(cas),
		writeBackManager:  writeBackManager,
		pctx:              pctx,
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
	err := s.backends.CheckReadiness()
	if err != nil {
		return handler.Errorf("not ready to serve traffic: %s", err).Status(http.StatusServiceUnavailable)
	}
	fmt.Fprintln(w, "OK")
	return nil
}

// statHandler returns blob info if it exists.
func (s *Server) statHandler(w http.ResponseWriter, r *http.Request) error {
	checkLocal, err := strconv.ParseBool(httputil.GetQueryArg(r, "local", "false"))
	if err != nil {
		return handler.Errorf("parse arg `local` as bool: %s", err)
	}
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}

	bi, err := s.stat(namespace, d, checkLocal)
	if os.IsNotExist(err) {
		log.With("namespace", namespace, "digest", d.Hex(), "local", checkLocal).Debug("Blob not found")
		return handler.ErrorStatus(http.StatusNotFound)
	} else if err != nil {
		log.With("namespace", namespace, "digest", d.Hex(), "local", checkLocal).Errorf("Failed to stat blob: %s", err)
		return fmt.Errorf("stat: %s", err)
	}
	w.Header().Set("Content-Length", strconv.FormatInt(bi.Size, 10))
	log.With("namespace", namespace, "digest", d.Hex(), "size", bi.Size).Debug("Successfully checked blob exists")
	return nil
}

func (s *Server) stat(namespace string, d core.Digest, checkLocal bool) (*core.BlobInfo, error) {
	fi, err := s.cas.GetCacheFileStat(d.Hex())
	if err == nil {
		log.With("namespace", namespace, "digest", d.Hex(), "size", fi.Size()).Debug("Found blob in local cache")
		return core.NewBlobInfo(fi.Size()), nil
	} else if os.IsNotExist(err) {
		if !checkLocal {
			log.With("namespace", namespace, "digest", d.Hex()).Debug("Blob not in local cache, checking backend")
			client, err := s.backends.GetClient(namespace)
			if err != nil {
				log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to get backend client: %s", err)
				return nil, fmt.Errorf("get backend client: %s", err)
			}
			if bi, err := client.Stat(namespace, d.Hex()); err == nil {
				log.With("namespace", namespace, "digest", d.Hex(), "size", bi.Size).Debug("Found blob in backend")
				return bi, nil
			} else if err == backenderrors.ErrBlobNotFound {
				log.With("namespace", namespace, "digest", d.Hex()).Debug("Blob not found in backend")
				return nil, os.ErrNotExist
			} else {
				log.With("namespace", namespace, "digest", d.Hex()).Errorf("Backend stat failed: %s", err)
				return nil, fmt.Errorf("backend stat: %s", err)
			}
		}
		return nil, err // os.ErrNotExist
	}

	log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to stat cache file: %s", err)
	return nil, fmt.Errorf("stat cache file: %s", err)
}

func (s *Server) downloadBlobHandler(w http.ResponseWriter, r *http.Request) error {
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	log.With("namespace", namespace, "digest", d.Hex()).Info("Starting blob download")
	if err := s.downloadBlob(namespace, d, w); err != nil {
		log.With("namespace", namespace, "digest", d.Hex()).Errorf("Error downloading blob: %s", err)
		return err
	}
	setOctetStreamContentType(w)
	log.With("namespace", namespace, "digest", d.Hex()).Info("Successfully downloaded blob")
	return nil
}

func (s *Server) replicateToRemoteHandler(w http.ResponseWriter, r *http.Request) error {
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	remote, err := httputil.ParseParam(r, "remote")
	if err != nil {
		return err
	}
	return s.replicateToRemote(namespace, d, remote)
}

func (s *Server) replicateToRemote(namespace string, d core.Digest, remoteDNS string) error {
	start := time.Now()

	fi, err := s.cas.GetCacheFileStat(d.Hex())
	if err != nil {
		if os.IsNotExist(err) {
			log.With("namespace", namespace, "digest", d.Hex(), "remote", remoteDNS).Info("Blob not in cache, starting remote download")
			return s.startRemoteBlobDownload(namespace, d, false)
		}
		log.With("namespace", namespace, "digest", d.Hex(), "remote", remoteDNS).Errorf("Failed to stat blob: %s", err)
		return handler.Errorf("stat blob: %s", err)
	}
	blobSize := fi.Size()

	log.With("namespace", namespace, "digest", d.Hex(), "remote", remoteDNS, "size_bytes", blobSize).Info("Starting replication to remote")
	f, err := s.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		log.With("namespace", namespace, "digest", d.Hex(), "remote", remoteDNS).Errorf("Failed to get cache file reader: %s", err)
		return handler.Errorf("file store: %s", err)
	}
	defer f.Close()

	remote, err := s.clusterProvider.Provide(remoteDNS)
	if err != nil {
		duration := time.Since(start)
		log.With("namespace", namespace, "digest", d.Hex(), "remote", remoteDNS, "size_bytes", blobSize, "duration_s", duration.Seconds()).Errorf("Failed to get remote cluster provider: %s", err)
		return handler.Errorf("remote cluster provider: %s", err)
	}
	if err := remote.UploadBlob(namespace, d, f); err != nil {
		duration := time.Since(start)
		log.With("namespace", namespace, "digest", d.Hex(), "remote", remoteDNS, "size_bytes", blobSize, "duration_s", duration.Seconds()).Errorf("Failed to upload blob to remote: %s", err)
		return err
	}
	duration := time.Since(start)
	log.With("namespace", namespace, "digest", d.Hex(), "remote", remoteDNS, "size_bytes", blobSize, "duration_s", duration.Seconds()).Info("Successfully replicated to remote")
	return nil
}

// deleteBlobHandler deletes blob data.
func (s *Server) deleteBlobHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	log.With("digest", d.Hex()).Info("Deleting blob")
	if err := s.deleteBlob(d); err != nil {
		log.With("digest", d.Hex()).Errorf("Failed to delete blob: %s", err)
		return err
	}
	setContentLength(w, 0)
	w.WriteHeader(http.StatusAccepted)
	log.With("digest", d.Hex()).Info("Successfully deleted blob")
	return nil
}

func (s *Server) getLocationsHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	locs := s.hashRing.Locations(d)
	w.Header().Set("Origin-Locations", strings.Join(locs, ","))
	w.WriteHeader(http.StatusOK)
	return nil
}

// getPeerContextHandler returns the Server's peer context as JSON.
func (s *Server) getPeerContextHandler(w http.ResponseWriter, r *http.Request) error {
	if err := json.NewEncoder(w).Encode(s.pctx); err != nil {
		return handler.Errorf("error converting peer context to json: %s", err)
	}
	return nil
}

func (s *Server) getMetaInfoHandler(w http.ResponseWriter, r *http.Request) error {
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	log.With("namespace", namespace, "digest", d.Hex()).Debug("Getting metainfo")
	raw, err := s.getMetaInfo(namespace, d)
	if err != nil {
		log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to get metainfo: %s", err)
		return err
	}
	w.Write(raw)
	log.With("namespace", namespace, "digest", d.Hex()).Debug("Successfully retrieved metainfo")
	return nil
}

func (s *Server) overwriteMetaInfoHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	pieceLength, err := strconv.ParseInt(r.URL.Query().Get("piece_length"), 10, 64)
	if err != nil {
		return handler.Errorf("invalid piece_length argument: %s", err).Status(http.StatusBadRequest)
	}
	log.With("digest", d.Hex(), "piece_length", pieceLength).Info("Overwriting metainfo")
	if err := s.overwriteMetaInfo(d, pieceLength); err != nil {
		log.With("digest", d.Hex(), "piece_length", pieceLength).Errorf("Failed to overwrite metainfo: %s", err)
		return err
	}
	log.With("digest", d.Hex(), "piece_length", pieceLength).Info("Successfully overwrote metainfo")
	return nil
}

// overwriteMetaInfo generates metainfo configured with pieceLength for d and
// writes it to disk, overwriting any existing metainfo. Primarily intended for
// benchmarking purposes.
func (s *Server) overwriteMetaInfo(d core.Digest, pieceLength int64) error {
	f, err := s.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		log.With("digest", d.Hex()).Errorf("Failed to get cache file for metainfo generation: %s", err)
		return handler.Errorf("get cache file: %s", err)
	}
	mi, err := core.NewMetaInfo(d, f, pieceLength)
	if err != nil {
		log.With("digest", d.Hex(), "piece_length", pieceLength).Errorf("Failed to create metainfo: %s", err)
		return handler.Errorf("create metainfo: %s", err)
	}
	if _, err := s.cas.SetCacheFileMetadata(d.Hex(), metadata.NewTorrentMeta(mi)); err != nil {
		log.With("digest", d.Hex()).Errorf("Failed to set metainfo: %s", err)
		return handler.Errorf("set metainfo: %s", err)
	}
	return nil
}

// getMetaInfo returns metainfo for d. If no blob exists under d, a download of
// the blob from the storage backend configured for namespace will be initiated.
// This download is asynchronous and getMetaInfo will immediately return a
// "202 Accepted" server error.
func (s *Server) getMetaInfo(namespace string, d core.Digest) ([]byte, error) {
	var tm metadata.TorrentMeta
	if err := s.cas.GetCacheFileMetadata(d.Hex(), &tm); os.IsNotExist(err) {
		log.With("namespace", namespace, "digest", d.Hex()).Debug("Metainfo not found in cache, initiating blob download")
		return nil, s.startRemoteBlobDownload(namespace, d, true)
	} else if err != nil {
		log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to get cache metadata: %s", err)
		return nil, handler.Errorf("get cache metadata: %s", err)
	}
	return tm.Serialize()
}

type localReplicationHook struct {
	server *Server
}

func (h *localReplicationHook) Run(d core.Digest) {
	start := time.Now()
	log.With("digest", d.Hex()).Info("Starting local replication")
	timer := h.server.stats.Timer("replicate_blob").Start()
	if err := h.server.replicateBlobLocally(d); err != nil {
		// Don't return error here as we only want to cache storage backend errors.
		duration := time.Since(start)
		log.With("digest", d.Hex(), "duration_s", duration.Seconds()).Errorf("Error replicating remote blob: %s", err)
		h.server.stats.Counter("replicate_blob_errors").Inc(1)
		return
	}
	timer.Stop()
	duration := time.Since(start)
	log.With("digest", d.Hex(), "duration_s", duration.Seconds()).Info("Successfully completed local replication")
}

func (s *Server) startRemoteBlobDownload(
	namespace string, d core.Digest, replicateLocally bool,
) error {
	log.With("namespace", namespace, "digest", d.Hex(), "replicate_locally", replicateLocally).Info("Initiating remote blob download")
	var hooks []blobrefresh.PostHook
	if replicateLocally {
		hooks = append(hooks, &localReplicationHook{s})
	}
	err := s.blobRefresher.Refresh(namespace, d, hooks...)
	switch err {
	case blobrefresh.ErrPending, nil:
		log.With("namespace", namespace, "digest", d.Hex()).Debug("Blob download pending or started")
		return handler.ErrorStatus(http.StatusAccepted)
	case blobrefresh.ErrNotFound:
		log.With("namespace", namespace, "digest", d.Hex()).Warn("Blob not found in backend")
		return handler.ErrorStatus(http.StatusNotFound)
	case blobrefresh.ErrWorkersBusy:
		log.With("namespace", namespace, "digest", d.Hex()).Warn("All blob refresh workers are busy")
		return handler.ErrorStatus(http.StatusServiceUnavailable)
	default:
		log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to start blob download: %s", err)
		return err
	}
}

func (s *Server) replicateBlobLocally(d core.Digest) error {
	fi, err := s.cas.GetCacheFileStat(d.Hex())
	var blobSize int64
	if err == nil {
		blobSize = fi.Size()
	}

	log.With("digest", d.Hex(), "size_bytes", blobSize).Debug("Starting replication to local replicas")
	return s.applyToReplicas(d, func(i int, client blobclient.Client) error {
		start := time.Now()
		f, err := s.cas.GetCacheFileReader(d.Hex())
		if err != nil {
			log.With("digest", d.Hex(), "replica", client.Addr()).Errorf("Failed to get cache reader: %s", err)
			return fmt.Errorf("get cache reader: %s", err)
		}
		if err := client.TransferBlob(d, f); err != nil {
			duration := time.Since(start)
			log.With("digest", d.Hex(), "replica", client.Addr(), "size_bytes", blobSize, "duration_s", duration.Seconds()).Errorf("Failed to transfer blob: %s", err)
			return fmt.Errorf("transfer blob: %s", err)
		}
		duration := time.Since(start)
		log.With("digest", d.Hex(), "replica", client.Addr(), "size_bytes", blobSize, "duration_s", duration.Seconds()).Debug("Successfully transferred blob to replica")
		return nil
	})
}

// applyToReplicas applies f to the replicas of d concurrently in random order,
// not including the current origin. Passes the index of the iteration to f.
func (s *Server) applyToReplicas(d core.Digest, f func(i int, c blobclient.Client) error) error {
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
	wg.Wait()

	return errutil.Join(errs)
}

// downloadBlob downloads blob for d into dst. If no blob exists under d, a
// download of the blob from the storage backend configured for namespace will
// be initiated. This download is asynchronous and downloadBlob will immediately
// return a "202 Accepted" handler error.
func (s *Server) downloadBlob(namespace string, d core.Digest, dst io.Writer) error {
	f, err := s.cas.GetCacheFileReader(d.Hex())
	if os.IsNotExist(err) {
		log.With("namespace", namespace, "digest", d.Hex()).Info("Blob not in cache, initiating download from backend")
		return s.startRemoteBlobDownload(namespace, d, true)
	} else if err != nil {
		log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to get cache file reader: %s", err)
		return handler.Errorf("get cache file: %s", err)
	}
	defer f.Close()

	if _, err := io.Copy(dst, f); err != nil {
		log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to copy blob data: %s", err)
		return handler.Errorf("copy blob: %s", err)
	}
	return nil
}

func (s *Server) deleteBlob(d core.Digest) error {
	if err := s.cas.DeleteCacheFile(d.Hex()); err != nil {
		if os.IsNotExist(err) {
			log.With("digest", d.Hex()).Warn("Attempted to delete non-existent blob")
			return handler.ErrorStatus(http.StatusNotFound)
		}
		log.With("digest", d.Hex()).Errorf("Failed to delete blob from cache: %s", err)
		return handler.Errorf("cannot delete blob data for digest %q: %s", d, err)
	}
	return nil
}

// startTransferHandler initializes an upload for internal blob transfers.
func (s *Server) startTransferHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	log.With("digest", d.Hex()).Debug("Starting internal transfer upload")
	if ok, err := blobExists(s.cas, d); err != nil {
		log.With("digest", d.Hex()).Errorf("Failed to check if blob exists: %s", err)
		return handler.Errorf("check blob: %s", err)
	} else if ok {
		log.With("digest", d.Hex()).Debug("Blob already exists, returning conflict")
		return handler.ErrorStatus(http.StatusConflict)
	}
	uid, err := s.uploader.start(d)
	if err != nil {
		log.With("digest", d.Hex()).Errorf("Failed to start upload: %s", err)
		return err
	}
	setUploadLocation(w, uid)
	w.WriteHeader(http.StatusOK)
	log.With("digest", d.Hex(), "uid", uid).Debug("Successfully started internal transfer upload")
	return nil
}

// patchTransferHandler uploads a chunk of a blob for internal uploads.
func (s *Server) patchTransferHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
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
	log.With("digest", d.Hex(), "uid", uid, "start", start, "end", end).Debug("Patching transfer upload chunk")
	if err := s.uploader.patch(d, uid, r.Body, start, end); err != nil {
		log.With("digest", d.Hex(), "uid", uid, "start", start, "end", end).Errorf("Failed to patch upload: %s", err)
		return err
	}
	log.With("digest", d.Hex(), "uid", uid, "start", start, "end", end).Debug("Successfully patched transfer upload chunk")
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
	log.With("digest", d.Hex(), "uid", uid).Info("Committing internal transfer upload")
	if err := s.uploader.commit(d, uid); err != nil {
		log.With("digest", d.Hex(), "uid", uid).Errorf("Failed to commit upload: %s", err)
		return err
	}
	if err := s.metaInfoGenerator.Generate(d); err != nil {
		log.With("digest", d.Hex(), "uid", uid).Errorf("Failed to generate metainfo: %s", err)
		return handler.Errorf("generate metainfo: %s", err)
	}
	log.With("digest", d.Hex(), "uid", uid).Info("Successfully committed internal transfer upload")
	return nil
}

func (s *Server) handleUploadConflict(err error, namespace string, d core.Digest) error {
	if herr, ok := err.(*handler.Error); ok && herr.GetStatus() == http.StatusConflict {
		// Even if the blob was already uploaded and committed to cache, it's
		// still possible that adding the write-back task failed. Clients short
		// circuit on conflict and return success, so we must make sure that if we
		// tell a client to stop before commit, the blob has been written back.
		log.With("namespace", namespace, "digest", d.Hex()).Debug("Handling upload conflict, ensuring write-back")
		if err := s.writeBack(namespace, d, 0); err != nil {
			log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to ensure write-back on conflict: %s", err)
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
	log.With("namespace", namespace, "digest", d.Hex()).Info("Starting cluster upload")
	uid, err := s.uploader.start(d)
	if err != nil {
		log.With("namespace", namespace, "digest", d.Hex()).Warnf("Failed to start cluster upload: %s", err)
		return s.handleUploadConflict(err, namespace, d)
	}
	setUploadLocation(w, uid)
	w.WriteHeader(http.StatusOK)
	log.With("namespace", namespace, "digest", d.Hex(), "uid", uid).Info("Successfully started cluster upload")
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
	log.With("namespace", namespace, "digest", d.Hex(), "uid", uid, "start", start, "end", end).Debug("Patching cluster upload chunk")
	if err := s.uploader.patch(d, uid, r.Body, start, end); err != nil {
		log.With("namespace", namespace, "digest", d.Hex(), "uid", uid).Errorf("Failed to patch cluster upload: %s", err)
		return s.handleUploadConflict(err, namespace, d)
	}
	log.With("namespace", namespace, "digest", d.Hex(), "uid", uid, "start", start, "end", end).Debug("Successfully patched upload chunk")
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

	log.With("namespace", namespace, "digest", d.Hex(), "uid", uid).Info("Committing cluster upload")
	if err := s.uploader.commit(d, uid); err != nil {
		log.With("namespace", namespace, "digest", d.Hex(), "uid", uid).Errorf("Failed to commit cluster upload: %s", err)
		return s.handleUploadConflict(err, namespace, d)
	}
	if err := s.writeBack(namespace, d, 0); err != nil {
		log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to write back blob: %s", err)
		return err
	}
	// Get blob size for replication logging
	fi, err := s.cas.GetCacheFileStat(d.Hex())
	var blobSize int64
	if err == nil {
		blobSize = fi.Size()
	}

	replicateStart := time.Now()
	log.With("namespace", namespace, "digest", d.Hex(), "size_bytes", blobSize).Debug("Replicating upload to other origins")
	err = s.applyToReplicas(d, func(i int, client blobclient.Client) error {
		replicaStart := time.Now()
		delay := s.config.DuplicateWriteBackStagger * time.Duration(i+1)
		f, err := s.cas.GetCacheFileReader(d.Hex())
		if err != nil {
			return fmt.Errorf("get cache file: %s", err)
		}
		if err := client.DuplicateUploadBlob(namespace, d, f, delay); err != nil {
			duration := time.Since(replicaStart)
			log.With("namespace", namespace, "digest", d.Hex(), "replica", client.Addr(), "size_bytes", blobSize, "duration_s", duration.Seconds()).Errorf("Failed to duplicate upload: %s", err)
			return fmt.Errorf("duplicate upload: %s", err)
		}
		duration := time.Since(replicaStart)
		log.With("namespace", namespace, "digest", d.Hex(), "replica", client.Addr(), "size_bytes", blobSize, "duration_s", duration.Seconds()).Debug("Successfully duplicated upload")
		return nil
	})
	if err != nil {
		s.stats.Counter("duplicate_write_back_errors").Inc(1)
		replicateDuration := time.Since(replicateStart)
		log.With("namespace", namespace, "digest", d.Hex(), "replication_duration_m", replicateDuration.Seconds()).Errorf("Error duplicating write-back task to replicas: %s", err)
	}
	log.With("namespace", namespace, "digest", d.Hex()).Info("Successfully committed cluster upload")
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
		log.With("namespace", namespace, "digest", d.Hex(), "uid", uid).Errorf("Failed to decode request body: %s", err)
		return handler.Errorf("decode body: %s", err)
	}
	delay := dr.Delay

	log.With("namespace", namespace, "digest", d.Hex(), "uid", uid, "delay", delay).Info("Committing duplicate upload")
	if err := s.uploader.commit(d, uid); err != nil {
		log.With("namespace", namespace, "digest", d.Hex(), "uid", uid).Errorf("Failed to commit duplicate upload: %s", err)
		return err
	}
	if err := s.writeBack(namespace, d, delay); err != nil {
		log.With("namespace", namespace, "digest", d.Hex(), "delay", delay).Errorf("Failed to write back duplicate: %s", err)
		return err
	}
	log.With("namespace", namespace, "digest", d.Hex(), "delay", delay).Info("Successfully committed duplicate upload")
	return nil
}

func (s *Server) writeBack(namespace string, d core.Digest, delay time.Duration) error {
	log.With("namespace", namespace, "digest", d.Hex(), "delay", delay).Debug("Starting write-back process")
	if _, err := s.cas.SetCacheFileMetadata(d.Hex(), metadata.NewPersist(true)); err != nil {
		log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to set persist metadata: %s", err)
		return handler.Errorf("set persist metadata: %s", err)
	}
	task := writeback.NewTask(namespace, d.Hex(), delay)
	if err := s.writeBackManager.Add(task); err != nil {
		log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to add write-back task: %s", err)
		return handler.Errorf("add write-back task: %s", err)
	}
	if err := s.metaInfoGenerator.Generate(d); err != nil {
		log.With("namespace", namespace, "digest", d.Hex()).Errorf("Failed to generate metainfo during write-back: %s", err)
		return handler.Errorf("generate metainfo: %s", err)
	}
	log.With("namespace", namespace, "digest", d.Hex()).Debug("Successfully scheduled write-back")
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

	log.With("ttl_hours", ttlHr).Info("Starting force cleanup")
	names, err := s.cas.ListCacheFiles()
	if err != nil {
		log.Errorf("Failed to list cache files for cleanup: %s", err)
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
	log.With("deleted_count", len(deleted), "error_count", len(errs), "ttl_hours", ttlHr).Info("Force cleanup completed")
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
		log.With("digest", name, "expired", expired, "owns", owns).Debug("Candidate for cleanup")
		// Ensure file is backed up properly before deleting.
		var pm metadata.Persist
		if err := s.cas.GetCacheFileMetadata(name, &pm); err != nil && !os.IsNotExist(err) {
			return false, fmt.Errorf("store: %s", err)
		}
		if pm.Value {
			// Note: It is possible that no writeback tasks exist, but the file
			// is persisted. We classify this as a leaked file which is safe to
			// delete.
			log.With("digest", name).Debug("File has persist metadata, executing write-back before cleanup")
			tasks, err := s.writeBackManager.Find(writeback.NewNameQuery(name))
			if err != nil {
				return false, fmt.Errorf("find writeback tasks: %s", err)
			}
			for _, task := range tasks {
				if err := s.writeBackManager.SyncExec(task); err != nil {
					log.With("digest", name).Errorf("Failed to execute write-back during cleanup: %s", err)
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
		log.With("digest", name, "expired", expired, "owns", owns).Info("Cleaned up blob")
		return true, nil
	}
	return false, nil
}
