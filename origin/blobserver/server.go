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

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/hashring"
	"code.uber.internal/infra/kraken/lib/metainfogen"
	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/lib/persistedretry/writeback"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/store/metadata"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/stringset"

	"github.com/pressly/chi"
	"github.com/uber-go/tally"
)

const _uploadChunkSize = 16 * memsize.MB

// Server defines a server that serves blob data for agent.
type Server struct {
	config            Config
	addr              string
	hashRing          hashring.Ring
	cas               *store.CAStore
	clientProvider    blobclient.Provider
	clusterProvider   blobclient.ClusterProvider
	stats             tally.Scope
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

	if !hashRing.Contains(addr) {
		return nil, fmt.Errorf("%s not found in hash ring", addr)
	}

	return &Server{
		config:            config,
		addr:              addr,
		hashRing:          hashRing,
		cas:               cas,
		clientProvider:    clientProvider,
		clusterProvider:   clusterProvider,
		stats:             stats,
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

	r.Get("/blobs/:digest/locations", handler.Wrap(s.getLocationsHandler))

	r.Post("/namespace/:namespace/blobs/:digest/uploads", handler.Wrap(s.startClusterUploadHandler))
	r.Patch("/namespace/:namespace/blobs/:digest/uploads/:uid", handler.Wrap(s.patchUploadHandler))
	r.Put("/namespace/:namespace/blobs/:digest/uploads/:uid", handler.Wrap(s.commitClusterUploadHandler))

	r.Get("/namespace/:namespace/blobs/:digest", handler.Wrap(s.downloadBlobHandler))

	r.Post("/namespace/:namespace/blobs/:digest/remote/:remote", handler.Wrap(s.replicateToRemoteHandler))

	// Internal endpoints:

	r.Post("/internal/blobs/:digest/uploads", handler.Wrap(s.startTransferHandler))
	r.Patch("/internal/blobs/:digest/uploads/:uid", handler.Wrap(s.patchUploadHandler))
	r.Put("/internal/blobs/:digest/uploads/:uid", handler.Wrap(s.commitTransferHandler))

	r.Delete("/internal/blobs/:digest", handler.Wrap(s.deleteBlobHandler))

	r.Post("/internal/blobs/:digest/metainfo", handler.Wrap(s.overwriteMetaInfoHandler))

	r.Get("/internal/peercontext", handler.Wrap(s.getPeerContextHandler))

	r.Head("/internal/namespace/:namespace/blobs/:digest", handler.Wrap(s.statHandler))

	r.Get("/internal/namespace/:namespace/blobs/:digest/metainfo", handler.Wrap(s.getMetaInfoHandler))

	r.Put(
		"/internal/duplicate/namespace/:namespace/blobs/:digest/uploads/:uid",
		handler.Wrap(s.duplicateCommitClusterUploadHandler))

	r.Mount("/", http.DefaultServeMux) // Serves /debug/pprof endpoints.

	return r
}

func (s *Server) healthCheckHandler(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(http.StatusOK)
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
	if err := s.ensureCorrectNode(d); err != nil {
		return err
	}

	bi, err := s.stat(namespace, d, checkLocal)
	if os.IsNotExist(err) {
		return handler.ErrorStatus(http.StatusNotFound)
	} else if err != nil {
		return fmt.Errorf("stat: %s", err)
	}
	w.Header().Set("Content-Length", strconv.FormatInt(bi.Size, 10))
	log.Debugf("successfully check blob %s exists", d.Hex())
	return nil
}

func (s *Server) stat(namespace string, d core.Digest, checkLocal bool) (*core.BlobInfo, error) {
	fi, err := s.cas.GetCacheFileStat(d.Hex())
	if err == nil {
		return core.NewBlobInfo(fi.Size()), nil
	} else if os.IsNotExist(err) {
		if !checkLocal {
			client, err := s.backends.GetClient(namespace)
			if err != nil {
				return nil, fmt.Errorf("get backend client: %s", err)
			}
			if bi, err := client.Stat(d.Hex()); err == nil {
				return bi, nil
			} else if err == backenderrors.ErrBlobNotFound {
				return nil, os.ErrNotExist
			} else {
				return nil, fmt.Errorf("backend stat: %s", err)
			}
		}
		return nil, err // os.ErrNotExist
	}

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
	if err := s.ensureCorrectNode(d); err != nil {
		return err
	}
	if err := s.downloadBlob(namespace, d, w); err != nil {
		return err
	}
	setOctetStreamContentType(w)
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
	f, err := s.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		if os.IsNotExist(err) {
			return s.startRemoteBlobDownload(namespace, d, false)
		}
		return handler.Errorf("file store: %s", err)
	}
	defer f.Close()

	remote, err := s.clusterProvider.Provide(remoteDNS)
	if err != nil {
		return handler.Errorf("remote cluster provider: %s", err)
	}
	return remote.UploadBlob(namespace, d, f)
}

// deleteBlobHandler deletes blob data.
func (s *Server) deleteBlobHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	if err := s.deleteBlob(d); err != nil {
		return err
	}
	setContentLength(w, 0)
	w.WriteHeader(http.StatusAccepted)
	log.Debugf("successfully delete blob %s", d.Hex())
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
	if err := s.ensureCorrectNode(d); err != nil {
		return err
	}
	raw, err := s.getMetaInfo(namespace, d)
	if err != nil {
		return err
	}
	w.Write(raw)
	return nil
}

func (s *Server) overwriteMetaInfoHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	if err := s.ensureCorrectNode(d); err != nil {
		return err
	}
	pieceLength, err := strconv.ParseInt(r.URL.Query().Get("piece_length"), 10, 64)
	if err != nil {
		return handler.Errorf("invalid piece_length argument: %s", err).Status(http.StatusBadRequest)
	}
	return s.overwriteMetaInfo(d, pieceLength)
}

// overwriteMetaInfo generates metainfo configured with pieceLength for d and
// writes it to disk, overwriting any existing metainfo. Primarily intended for
// benchmarking purposes.
func (s *Server) overwriteMetaInfo(d core.Digest, pieceLength int64) error {
	f, err := s.cas.GetCacheFileReader(d.Hex())
	if err != nil {
		return handler.Errorf("get cache file: %s", err)
	}
	mi, err := core.NewMetaInfoFromBlob(d.Hex(), f, pieceLength)
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
func (s *Server) getMetaInfo(namespace string, d core.Digest) ([]byte, error) {
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
	if err := h.server.replicateBlobLocally(d); err != nil {
		// Don't return error here as we only want to cache storage backend errors.
		log.With("blob", d.Hex()).Errorf("Error replicating remote blob: %s", err)
		h.server.stats.Counter("replicate_blob_errors").Inc(1)
		return
	}
	timer.Stop()
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
	return s.applyToReplicas(d, func(i int, client blobclient.Client) error {
		f, err := s.cas.GetCacheFileReader(d.Hex())
		if err != nil {
			return fmt.Errorf("get cache reader: %s", err)
		}
		if err := client.TransferBlob(d, f); err != nil {
			return fmt.Errorf("transfer blob: %s", err)
		}
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

func (s *Server) ensureCorrectNode(d core.Digest) error {
	for _, loc := range s.hashRing.Locations(d) {
		if loc == s.addr {
			return nil
		}
	}
	return handler.Errorf("digest does not hash to this origin").Status(http.StatusBadRequest)
}

// downloadBlob downloads blob for d into dst. If no blob exists under d, a
// download of the blob from the storage backend configured for namespace will
// be initiated. This download is asynchronous and downloadBlob will immediately
// return a "202 Accepted" handler error.
func (s *Server) downloadBlob(namespace string, d core.Digest, dst io.Writer) error {
	f, err := s.cas.GetCacheFileReader(d.Hex())
	if os.IsNotExist(err) {
		return s.startRemoteBlobDownload(namespace, d, true)
	} else if err != nil {
		return handler.Errorf("get cache file: %s", err)
	}
	defer f.Close()

	if _, err := io.Copy(dst, f); err != nil {
		return handler.Errorf("copy blob: %s", err)
	}
	return nil
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
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	if err := s.ensureCorrectNode(d); err != nil {
		return err
	}
	if ok, err := blobExists(s.cas, d); err != nil {
		return handler.Errorf("check blob: %s", err)
	} else if ok {
		return handler.ErrorStatus(http.StatusConflict)
	}
	uid, err := s.uploader.start(d)
	if err != nil {
		return err
	}
	setUploadLocation(w, uid)
	w.WriteHeader(http.StatusOK)
	return nil
}

// startClusterUploadHandler initializes an upload for external uploads.
func (s *Server) startClusterUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	if err := s.ensureCorrectNode(d); err != nil {
		return err
	}
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	if ok, err := blobExists(s.cas, d); err != nil {
		return handler.Errorf("check blob: %s", err)
	} else if ok {
		// Even if the blob was already uploaded and committed to cache, it's
		// still possible that adding the write-back task failed. In this scenario,
		// it's expected that the client might retry the entire upload (as opposed
		// to just retrying the commit). We try to write-back and return 504
		// just in case.
		if err := s.writeBack(namespace, d, 0); err != nil {
			return err
		}
		return handler.ErrorStatus(http.StatusConflict)
	}
	uid, err := s.uploader.start(d)
	if err != nil {
		return err
	}
	setUploadLocation(w, uid)
	w.WriteHeader(http.StatusOK)
	return nil
}

// patchUploadHandler uploads a chunk of a blob for both internal and external uploads.
func (s *Server) patchUploadHandler(w http.ResponseWriter, r *http.Request) error {
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
	return s.uploader.patch(d, uid, r.Body, start, end)
}

// commitTransferHandler commits the upload of an internal blob transfer.
// Internal blob transfers are not replicated to the rest of the cluster.
func (s *Server) commitTransferHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	if err := s.ensureCorrectNode(d); err != nil {
		return err
	}
	uid, err := httputil.ParseParam(r, "uid")
	if err != nil {
		return err
	}

	if err := s.uploader.verify(d, uid); err != nil {
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

// commitClusterUploadHandler commits an external blob upload asynchronously,
// meaning the blob will be written back to remote storage in a non-blocking
// fashion.
func (s *Server) commitClusterUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return err
	}
	if err := s.ensureCorrectNode(d); err != nil {
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

	if err := s.uploader.verify(d, uid); err != nil {
		return err
	}
	if err := s.uploader.commit(d, uid); err != nil {
		return err
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
	if err := s.ensureCorrectNode(d); err != nil {
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

	if err := s.uploader.verify(d, uid); err != nil {
		return err
	}
	if err := s.uploader.commit(d, uid); err != nil {
		return err
	}
	return s.writeBack(namespace, d, delay)
}

func (s *Server) writeBack(namespace string, d core.Digest, delay time.Duration) error {
	if _, err := s.cas.SetCacheFileMetadata(d.Hex(), metadata.NewPersist(true)); err != nil {
		return handler.Errorf("set persist metadata: %s", err)
	}
	task := writeback.NewTaskWithDelay(namespace, d.Hex(), delay)
	if err := s.writeBackManager.Add(task); err != nil {
		return handler.Errorf("add write-back task: %s", err)
	}
	if err := s.metaInfoGenerator.Generate(d); err != nil {
		return handler.Errorf("generate metainfo: %s", err)
	}
	return nil
}
