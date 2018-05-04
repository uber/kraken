package blobserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/lib/metainfogen"
	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"

	"github.com/docker/distribution/uuid"
	"github.com/pressly/chi"
	"github.com/uber-go/tally"
)

const _uploadChunkSize = 16 * memsize.MB

// Server defines a server that serves blob data for agent.
type Server struct {
	config            Config
	label             string
	addr              string
	labelToAddr       map[string]string
	hashState         *hrw.RendezvousHash
	fileStore         store.OriginFileStore
	clientProvider    blobclient.Provider
	stats             tally.Scope
	backends          *backend.Manager
	blobRefresher     *blobrefresh.Refresher
	metaInfoGenerator *metainfogen.Generator
	uploader          *uploader

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
	fileStore store.OriginFileStore,
	clientProvider blobclient.Provider,
	pctx core.PeerContext,
	backends *backend.Manager,
	blobRefresher *blobrefresh.Refresher,
	metaInfoGenerator *metainfogen.Generator) (*Server, error) {

	stats = stats.Tagged(map[string]string{
		"module": "blobserver",
	})

	if len(config.HashNodes) == 0 {
		return nil, errors.New("no hash nodes configured")
	}

	currNode, ok := config.HashNodes[addr]
	if !ok {
		return nil, fmt.Errorf("host address %s not in configured hash nodes", addr)
	}
	label := currNode.Label

	return &Server{
		config:            config,
		label:             label,
		addr:              addr,
		labelToAddr:       config.LabelToAddress(),
		hashState:         config.HashState(),
		fileStore:         fileStore,
		clientProvider:    clientProvider,
		stats:             stats,
		backends:          backends,
		blobRefresher:     blobRefresher,
		metaInfoGenerator: metaInfoGenerator,
		uploader:          newUploader(fileStore),
		pctx:              pctx,
	}, nil
}

// Addr returns the address the blob server is configured on.
func (s Server) Addr() string {
	return s.addr
}

// Handler returns an http handler for the blob server.
func (s Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	// Public endpoints:

	r.Get("/health", handler.Wrap(s.healthCheckHandler))

	r.Get("/blobs/:digest/locations", handler.Wrap(s.getLocationsHandler))

	r.Post("/namespace/:namespace/blobs/:digest/uploads", handler.Wrap(s.startUploadHandler))
	r.Patch("/namespace/:namespace/blobs/:digest/uploads/:uid", handler.Wrap(s.patchUploadHandler))
	r.Put("/namespace/:namespace/blobs/:digest/uploads/:uid", handler.Wrap(s.commitClusterUploadHandler))

	r.Get("/namespace/:namespace/blobs/:digest", handler.Wrap(s.downloadBlobHandler))

	r.Post("/namespace/:namespace/blobs/:digest/remote/:remote", handler.Wrap(s.replicateToRemoteHandler))

	// Internal endpoints:

	r.Post("/internal/blobs/:digest/uploads", handler.Wrap(s.startUploadHandler))
	r.Patch("/internal/blobs/:digest/uploads/:uid", handler.Wrap(s.patchUploadHandler))
	r.Put("/internal/blobs/:digest/uploads/:uid", handler.Wrap(s.commitTransferHandler))

	r.Head("/internal/blobs/:digest", handler.Wrap(s.checkBlobHandler))
	r.Delete("/internal/blobs/:digest", handler.Wrap(s.deleteBlobHandler))

	r.Post("/internal/blobs/:digest/metainfo", handler.Wrap(s.overwriteMetaInfoHandler))

	r.Post("/internal/repair", handler.Wrap(s.repairHandler))
	r.Post("/internal/repair/shard/:shardid", handler.Wrap(s.repairShardHandler))
	r.Post("/internal/repair/digest/:digest", handler.Wrap(s.repairDigestHandler))

	r.Get("/internal/peercontext", handler.Wrap(s.getPeerContextHandler))

	r.Get("/internal/namespace/:namespace/blobs/:digest/metainfo", handler.Wrap(s.getMetaInfoHandler))

	r.Mount("/", http.DefaultServeMux) // Serves /debug/pprof endpoints.

	return r
}

func (s Server) healthCheckHandler(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(http.StatusOK)
	return nil
}

// checkBlobHandler checks if blob data exists.
func (s Server) checkBlobHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	if err := s.ensureCorrectNode(d); err != nil {
		return err
	}
	if ok, err := blobExists(s.fileStore, d); err != nil {
		return err
	} else if !ok {
		return handler.ErrorStatus(http.StatusNotFound)
	}
	w.WriteHeader(http.StatusOK)
	log.Debugf("successfully check blob %s exists", d.Hex())
	return nil
}

func (s Server) downloadBlobHandler(w http.ResponseWriter, r *http.Request) error {
	namespace, err := parseNamespace(r)
	if err != nil {
		return err
	}
	d, err := parseDigest(r)
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

func (s Server) replicateToRemoteHandler(w http.ResponseWriter, r *http.Request) error {
	namespace, err := parseNamespace(r)
	if err != nil {
		return err
	}
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	remoteDNS := chi.URLParam(r, "remote")
	return s.replicateToRemote(namespace, d, remoteDNS)
}

func (s Server) replicateToRemote(namespace string, d core.Digest, remoteDNS string) error {
	f, err := s.fileStore.GetCacheFileReader(d.Hex())
	if err != nil {
		if os.IsNotExist(err) {
			return s.startRemoteBlobDownload(namespace, d, false)
		}
		return handler.Errorf("file store: %s", err)
	}
	defer f.Close()

	remoteCluster := blobclient.NewClusterClient(
		blobclient.NewClientResolver(s.clientProvider, remoteDNS))

	return remoteCluster.UploadBlob(namespace, d, f, true)
}

// deleteBlobHandler deletes blob data.
func (s Server) deleteBlobHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
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

func (s Server) getLocationsHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	locs, err := s.getLocations(d)
	if err != nil {
		return err
	}
	w.Header().Set("Origin-Locations", strings.Join(locs, ","))
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s Server) repairHandler(w http.ResponseWriter, r *http.Request) error {
	shards, err := s.fileStore.ListPopulatedShardIDs()
	if err != nil {
		return handler.Errorf("failed to list populated shard ids: %s", err)
	}
	rep := s.newRepairer()
	go func() {
		defer rep.Close()
		for _, shardID := range shards {
			err = rep.RepairShard(shardID)
			if err != nil {
				return
			}
		}
	}()
	rep.WriteMessages(w)
	log.Debugf("successfully repair owning shards %v", shards)
	return err
}

func (s Server) repairShardHandler(w http.ResponseWriter, r *http.Request) error {
	shardID := chi.URLParam(r, "shardid")
	if len(shardID) == 0 {
		return handler.Errorf("empty shard id").Status(http.StatusBadRequest)
	}
	rep := s.newRepairer()
	var err error
	go func() {
		defer rep.Close()
		err = rep.RepairShard(shardID)
	}()
	rep.WriteMessages(w)
	log.Debugf("successfully repair shard %s", shardID)
	return err
}

func (s Server) repairDigestHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	rep := s.newRepairer()
	go func() {
		defer rep.Close()
		err = rep.RepairDigest(d)
	}()
	rep.WriteMessages(w)
	log.Debugf("successfully repair digest %s", d.Hex())
	return err
}

// getPeerContextHandler returns the Server's peer context as JSON.
func (s Server) getPeerContextHandler(w http.ResponseWriter, r *http.Request) error {
	if err := json.NewEncoder(w).Encode(s.pctx); err != nil {
		return handler.Errorf("error converting peer context to json: %s", err)
	}
	return nil
}

func (s Server) getMetaInfoHandler(w http.ResponseWriter, r *http.Request) error {
	namespace, err := parseNamespace(r)
	if err != nil {
		return err
	}
	d, err := parseDigest(r)
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

func (s Server) overwriteMetaInfoHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
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
func (s Server) overwriteMetaInfo(d core.Digest, pieceLength int64) error {
	f, err := s.fileStore.GetCacheFileReader(d.Hex())
	if err != nil {
		return handler.Errorf("get cache file: %s", err)
	}
	mi, err := core.NewMetaInfoFromBlob(d.Hex(), f, pieceLength)
	if err != nil {
		return handler.Errorf("create metainfo: %s", err)
	}
	raw, err := mi.Serialize()
	if err != nil {
		return handler.Errorf("serialize metainfo: %s", err)
	}
	if _, err := s.fileStore.SetCacheFileMetadata(d.Hex(), store.NewTorrentMeta(), raw); err != nil {
		return handler.Errorf("set metainfo: %s", err)
	}
	return nil
}

// getMetaInfo returns metainfo for d. If no blob exists under d, a download of
// the blob from the storage backend configured for namespace will be initiated.
// This download is asynchronous and getMetaInfo will immediately return a
// "202 Accepted" server error.
func (s Server) getMetaInfo(namespace string, d core.Digest) ([]byte, error) {
	raw, err := s.fileStore.GetCacheFileMetadata(d.Hex(), store.NewTorrentMeta())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, s.startRemoteBlobDownload(namespace, d, true)
		}
		return nil, handler.Errorf("get cache metadata: %s", err)
	}
	return raw, nil
}

type localReplicationHook struct {
	server Server
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

func (s Server) startRemoteBlobDownload(
	namespace string, d core.Digest, replicateLocally bool) error {

	// Always check whether the blob is actually available for download before
	// returning a potential 202. This ensures that the majority of errors are
	// propogated quickly and syncronously.
	client, err := s.backends.GetClient(namespace)
	if err != nil {
		return fmt.Errorf("backend manager: %s", err)
	}
	if _, err := client.Stat(d.Hex()); err != nil {
		if err == backenderrors.ErrBlobNotFound {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return fmt.Errorf("stat: %s", err)
	}

	var hooks []blobrefresh.PostHook
	if replicateLocally {
		hooks = append(hooks, &localReplicationHook{s})
	}
	err = s.blobRefresher.Refresh(namespace, d, hooks...)
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

func (s Server) downloadRemoteBlob(c backend.Client, d core.Digest) error {
	u := uuid.Generate().String()
	if err := s.fileStore.CreateUploadFile(u, 0); err != nil {
		return handler.Errorf("create upload file: %s", err)
	}
	f, err := s.fileStore.GetUploadFileReadWriter(u)
	if err != nil {
		return handler.Errorf("get upload writer: %s", err)
	}
	if err := c.Download(d.Hex(), f); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return handler.Errorf("seek: %s", err)
	}
	fd, err := core.NewDigester().FromReader(f)
	if err != nil {
		return handler.Errorf("compute digest: %s", err)
	}
	if fd != d {
		return handler.Errorf("invalid remote blob digest: got %s, expected %s", fd, d)
	}
	if err := s.fileStore.MoveUploadFileToCache(u, d.Hex()); err != nil {
		return handler.Errorf("move upload file to cache: %s", err)
	}
	return nil
}

func (s Server) replicateBlobLocally(d core.Digest) error {
	locs, err := s.getLocations(d)
	if err != nil {
		return fmt.Errorf("get locations: %s", err)
	}

	var mu sync.Mutex
	var errs []error

	var wg sync.WaitGroup
	for _, loc := range locs {
		if s.addr == loc {
			continue
		}
		wg.Add(1)
		go func(loc string) {
			defer wg.Done()
			if err := transferBlob(s.fileStore, d, s.clientProvider.Provide(loc)); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(loc)
	}
	wg.Wait()

	return errutil.Join(errs)
}

func (s Server) getLocations(d core.Digest) ([]string, error) {
	nodes, err := s.hashState.GetOrderedNodes(d.ShardID(), s.config.NumReplica)
	if err != nil || len(nodes) == 0 {
		return nil, handler.Errorf("get nodes: %s", err)
	}
	var locs []string
	for _, node := range nodes {
		locs = append(locs, s.labelToAddr[node.Label])
	}
	sort.Strings(locs)
	return locs, nil
}

func (s Server) ensureCorrectNode(d core.Digest) error {
	nodes, err := s.hashState.GetOrderedNodes(d.ShardID(), s.config.NumReplica)
	if err != nil || len(nodes) == 0 {
		return handler.Errorf("get nodes: %s", err)
	}
	for _, node := range nodes {
		if node.Label == s.label {
			return nil
		}
	}
	return handler.Errorf("digest does not hash to this origin").Status(http.StatusBadRequest)
}

// downloadBlob downloads blob for d into dst. If no blob exists under d, a
// download of the blob from the storage backend configured for namespace will
// be initiated. This download is asynchronous and downloadBlob will immediately
// return a "202 Accepted" handler error.
func (s Server) downloadBlob(namespace string, d core.Digest, dst io.Writer) error {
	f, err := s.fileStore.GetCacheFileReader(d.Hex())
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

func (s Server) deleteBlob(d core.Digest) error {
	if err := s.fileStore.DeleteCacheFile(d.Hex()); err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("cannot delete blob data for digest %q: %s", d, err)
	}
	return nil
}

// startUploadHandler initializes an upload for both internal and external uploads.
// Returns the location of the upload which is needed for subsequent chunk uploads of
// this blob.
func (s Server) startUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	if err := s.ensureCorrectNode(d); err != nil {
		return err
	}
	uid, err := s.uploader.start(d)
	if err != nil {
		return err
	}
	setUploadLocation(w, uid)
	return nil
}

// patchUploadHandler uploads a chunk of a blob for both internal and external uploads.
func (s Server) patchUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	uid := chi.URLParam(r, "uid")
	start, end, err := parseContentRange(r.Header)
	if err != nil {
		return err
	}
	return s.uploader.patch(d, uid, r.Body, start, end)
}

// commitTransferHandler commits the upload of an internal blob transfer.
// Internal blob transfers are not replicated to the rest of the cluster.
func (s Server) commitTransferHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	if err := s.ensureCorrectNode(d); err != nil {
		return err
	}
	uid := chi.URLParam(r, "uid")
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

// commitClusterUploadHandler commits the upload of an external blob upload.
// External blob uploads support write-through operations to storage backends
// and automatically replicate to the rest of the cluster.
func (s Server) commitClusterUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	if err := s.ensureCorrectNode(d); err != nil {
		return err
	}
	namespace, err := parseNamespace(r)
	if err != nil {
		return err
	}
	uid := chi.URLParam(r, "uid")
	through := false
	t := r.URL.Query().Get("through")
	if t != "" {
		through, err = strconv.ParseBool(t)
		if err != nil {
			return handler.Errorf(
				"invalid through argument: parse bool: %s", err).Status(http.StatusBadRequest)
		}
	}
	if err := s.commitClusterUpload(uid, namespace, d, through); err != nil {
		return err
	}
	if err := s.metaInfoGenerator.Generate(d); err != nil {
		return handler.Errorf("generate metainfo: %s", err)
	}
	return nil
}

func (s Server) commitClusterUpload(
	uid string, namespace string, d core.Digest, through bool) error {

	if err := s.uploader.verify(d, uid); err != nil {
		return err
	}
	f, err := s.fileStore.GetUploadFileReader(uid)
	if err != nil {
		return handler.Errorf("get upload file: %s", err)
	}

	// If through is set, we must make sure we safely upload the file to namespace's
	// storage backend before committing the file to the cache. If the file can't be
	// uploaded to said backend, the entire upload operation must fail.
	if through {
		c, err := s.backends.GetClient(namespace)
		if err != nil {
			return handler.Errorf("backend manager: %s", err).Status(http.StatusBadRequest)
		}
		log.With("blob", d.Hex()).Infof("Uploading blob to %s backend", namespace)
		if err := c.Upload(d.Hex(), f); err != nil {
			// TODO(codyg): We need some way of detecting whether the blob already exists
			// in the storage backend.
			return handler.Errorf("backend upload: %s", err)
		}
	}

	if err := s.uploader.commit(d, uid); err != nil {
		return err
	}
	if err := s.replicateBlobLocally(d); err != nil {
		log.With("blob", d.Hex()).Errorf("Error replicating uploaded blob: %s", err)
	}

	return nil
}

func (s Server) newRepairer() *repairer {
	return newRepairer(
		context.TODO(),
		s.config,
		s.addr,
		s.labelToAddr,
		s.hashState,
		s.fileStore,
		s.clientProvider)
}
