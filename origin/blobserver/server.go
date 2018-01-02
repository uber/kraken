package blobserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/lib/middleware"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"

	"github.com/andres-erbsen/clock"
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
	fileStore         store.FileStore
	clientProvider    blobclient.Provider
	stats             tally.Scope
	backendManager    *backend.Manager
	requestCache      *dedup.RequestCache
	pieceLengthConfig *pieceLengthConfig

	// This is an unfortunate coupling between the p2p client and the blob server.
	// Tracker queries the origin cluster to discover which origins can seed
	// a given torrent, however this requires blob server to understand the
	// context of the p2p client running alongside it.
	pctx peercontext.PeerContext
}

// New initializes a new Server.
func New(
	config Config,
	stats tally.Scope,
	addr string,
	fileStore store.FileStore,
	clientProvider blobclient.Provider,
	pctx peercontext.PeerContext,
	backendManager *backend.Manager) (*Server, error) {

	if len(config.HashNodes) == 0 {
		return nil, errors.New("no hash nodes configured")
	}

	currNode, ok := config.HashNodes[addr]
	if !ok {
		return nil, fmt.Errorf("host address %s not in configured hash nodes", addr)
	}
	label := currNode.Label

	plConfig, err := newPieceLengthConfig(config.PieceLengths)
	if err != nil {
		return nil, fmt.Errorf("piece length config: %s", err)
	}

	return &Server{
		config:            config,
		label:             label,
		addr:              addr,
		labelToAddr:       config.LabelToAddress(),
		hashState:         config.HashState(),
		fileStore:         fileStore,
		clientProvider:    clientProvider,
		stats:             stats.SubScope("blobserver"),
		backendManager:    backendManager,
		requestCache:      dedup.NewRequestCache(config.RequestCache, clock.New()),
		pieceLengthConfig: plConfig,
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

	r.Group(func(r chi.Router) {
		stats := s.stats.SubScope("health")
		r.Use(middleware.Counter(stats))
		r.Use(middleware.ElapsedTimer(stats))

		r.Get("/health", handler.Wrap(s.healthCheckHandler))
	})

	r.Group(func(r chi.Router) {
		stats := s.stats.SubScope("blobs")
		r.Use(middleware.Counter(stats))
		r.Use(middleware.ElapsedTimer(stats))

		r.Head("/blobs/:digest", handler.Wrap(s.checkBlobHandler))
		r.Get("/blobs/:digest", handler.Wrap(s.getBlobHandler))
		r.Delete("/blobs/:digest", handler.Wrap(s.deleteBlobHandler))
	})

	r.Group(func(r chi.Router) {
		stats := s.stats.SubScope("blobs.locations")

		r.Use(middleware.Counter(stats))
		r.Use(middleware.ElapsedTimer(stats))

		r.Get("/blobs/:digest/locations", handler.Wrap(s.getLocationsHandler))
	})

	r.Group(func(r chi.Router) {
		stats := s.stats.SubScope("blobs.uploads")
		r.Use(middleware.Counter(stats))
		r.Use(middleware.ElapsedTimer(stats))

		r.Post("/blobs/:digest/uploads", handler.Wrap(s.startUploadHandler))
		r.Patch("/blobs/:digest/uploads/:uuid", handler.Wrap(s.patchUploadHandler))
		r.Put("/blobs/:digest/uploads/:uuid", handler.Wrap(s.commitUploadHandler))
	})

	r.Group(func(r chi.Router) {
		stats := s.stats.SubScope("repair")
		r.Use(middleware.Counter(stats))
		r.Use(middleware.ElapsedTimer(stats))

		r.Post("/repair", handler.Wrap(s.repairHandler))
	})

	r.Group(func(r chi.Router) {
		stats := s.stats.SubScope("repair.shard")
		r.Use(middleware.Counter(stats))
		r.Use(middleware.ElapsedTimer(stats))

		r.Post("/repair/shard/:shardid", handler.Wrap(s.repairShardHandler))
	})

	r.Group(func(r chi.Router) {
		stats := s.stats.SubScope("repair.digest")
		r.Use(middleware.Counter(stats))
		r.Use(middleware.ElapsedTimer(stats))

		r.Post("/repair/digest/:digest", handler.Wrap(s.repairDigestHandler))
	})

	r.Group(func(r chi.Router) {
		stats := s.stats.SubScope("peercontext")
		r.Use(middleware.Counter(stats))
		r.Use(middleware.ElapsedTimer(stats))

		r.Get("/peercontext", handler.Wrap(s.getPeerContextHandler))
	})

	r.Group(func(r chi.Router) {
		stats := s.stats.SubScope("namespace.blobs.metainfo")
		r.Use(middleware.Counter(stats))
		r.Use(middleware.ElapsedTimer(stats))

		r.Get("/namespace/:namespace/blobs/:digest/metainfo", handler.Wrap(s.getMetaInfoHandler))
	})

	// Serves /debug/pprof endpoints.
	r.Mount("/", http.DefaultServeMux)

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
	if err := s.redirectByDigest(d); err != nil {
		return err
	}
	if err := s.ensureDigestExists(d); err != nil {
		return err
	}
	w.WriteHeader(http.StatusOK)
	log.Debugf("successfully check blob %s exists", d.Hex())
	return nil
}

// getBlobHandler gets blob data.
func (s Server) getBlobHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	if err := s.redirectByDigest(d); err != nil {
		return err
	}
	if err := s.downloadBlob(d, w); err != nil {
		return err
	}
	setOctetStreamContentType(w)
	log.Debugf("successfully get blob %s", d.Hex())
	return nil
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
	log.Debugf("successfully get location for blob %s: locs: %v", d.Hex(), locs)
	return nil
}

// startUploadHandler starts upload process for a blob. Returns the location of
// the upload which is needed for subsequent patches of this blob.
func (s Server) startUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}

	if err := s.redirectByDigest(d); err != nil {
		return err
	}
	if err := s.ensureDigestNotExists(d); err != nil {
		return err
	}
	u, err := s.createUpload(d)
	if err != nil {
		return err
	}
	setUploadLocation(w, u)
	setContentLength(w, 0)
	w.WriteHeader(http.StatusAccepted)
	log.Debugf("successfully start upload %s for blob %s", u, d.Hex())
	return nil
}

// patchUploadHandler uploads a chunk of a blob.
func (s Server) patchUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	u, err := parseUUID(r)
	if err != nil {
		return err
	}
	if err := s.redirectByDigest(d); err != nil {
		return err
	}
	if err := s.ensureDigestNotExists(d); err != nil {
		return err
	}
	start, end, err := parseContentRange(r.Header)
	if err != nil {
		return err
	}

	if err := s.uploadBlobChunk(u, r.Body, start, end); err != nil {
		return err
	}
	setContentLength(w, 0)
	w.WriteHeader(http.StatusAccepted)
	log.Debugf("successfully patch upload %s for blob %s", u, d.Hex())
	return nil
}

// commitUploadHandler commits the upload.
func (s Server) commitUploadHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	u, err := parseUUID(r)
	if err != nil {
		return err
	}
	if err := s.redirectByDigest(d); err != nil {
		return err
	}
	if err := s.commitUpload(d, u); err != nil {
		return err
	}

	setContentLength(w, 0)
	w.WriteHeader(http.StatusCreated)
	log.Debugf("successfully commit upload %s for blob %s", u, d.Hex())
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
	namespace := chi.URLParam(r, "namespace")
	if len(namespace) == 0 {
		return handler.Errorf("empty namespace").Status(http.StatusBadRequest)
	}
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	raw, err := s.getMetaInfo(namespace, d)
	if err != nil {
		return err
	}
	w.Write(raw)
	return nil
}

// getMetaInfo returns metainfo for d. If no blob exists under d, a download of
// the blob from the storage backend configured for namespace will be initiated.
// This download is asynchronous and getMetaInfo will immediately return a
// "202 Accepted" server error.
func (s Server) getMetaInfo(namespace string, d image.Digest) ([]byte, error) {
	if _, err := s.fileStore.GetCacheFileStat(d.Hex()); os.IsNotExist(err) {
		return nil, s.startRemoteBlobDownload(namespace, d)
	} else if err != nil {
		return nil, handler.Errorf("cache file stat: %s", err)
	}
	cache := s.fileStore.States().Cache()
	raw, err := cache.GetMetadata(d.Hex(), store.NewTorrentMeta())
	if os.IsNotExist(err) {
		raw, err = s.generateMetaInfo(d)
		if err != nil {
			return nil, handler.Errorf("generate metainfo: %s", err)
		}
		// Never overwrite existing metadata.
		raw, err = cache.GetOrSetMetadata(d.Hex(), store.NewTorrentMeta(), raw)
		if err != nil {
			return nil, handler.Errorf("get or set metadata: %s", err)
		}
	} else if err != nil {
		return nil, handler.Errorf("get cache metadata: %s", err)
	}
	return raw, nil
}

func (s Server) startRemoteBlobDownload(namespace string, d image.Digest) error {
	c, err := s.backendManager.GetClient(namespace)
	if err != nil {
		return handler.Errorf("backend manager: %s", err).Status(http.StatusBadRequest)
	}
	id := namespace + ":" + d.Hex()
	err = s.requestCache.Start(id, func() error {
		if err := s.downloadRemoteBlob(c, d); err != nil {
			if err == backend.ErrBlobNotFound {
				return dedup.ErrNotFound
			}
			return err
		}
		// Replicate the blob within the request cache worker, but don't return any
		// errors because we only want to cache storage backend errors.
		if err := s.replicateBlob(d); err != nil {
			log.With("blob", d.Hex()).Errorf("Error replicating remote blob: %s", err)
		}
		return nil
	})
	if err == dedup.ErrRequestPending || err == nil {
		return handler.ErrorStatus(http.StatusAccepted)
	} else if err == dedup.ErrNotFound {
		return handler.ErrorStatus(http.StatusNotFound)
	} else if err == dedup.ErrWorkersBusy {
		return handler.ErrorStatus(http.StatusServiceUnavailable)
	}
	return err
}

func (s Server) downloadRemoteBlob(c backend.Client, d image.Digest) error {
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
	fd, err := image.NewDigester().FromReader(f)
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

func (s Server) replicateBlob(d image.Digest) error {
	locs, err := s.getLocations(d)
	if err != nil {
		return handler.Errorf("get locations: %s", err)
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
			if err := s.pushBlob(loc, d); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(loc)
	}
	wg.Wait()

	return errutil.Join(errs)
}

func (s Server) pushBlob(loc string, d image.Digest) error {
	info, err := s.fileStore.GetCacheFileStat(d.Hex())
	if err != nil {
		return handler.Errorf("cache stat: %s", err)
	}
	f, err := s.fileStore.GetCacheFileReader(d.Hex())
	if err != nil {
		return handler.Errorf("get cache reader: %s", err)
	}
	if err := s.clientProvider.Provide(loc).PushBlob(d, f, info.Size()); err != nil {
		return handler.Errorf("push blob: %s", err)
	}
	return nil
}

func (s Server) generateMetaInfo(d image.Digest) ([]byte, error) {
	info, err := s.fileStore.GetCacheFileStat(d.Hex())
	if err != nil {
		return nil, handler.Errorf("cache stat: %s", err)
	}
	f, err := s.fileStore.GetCacheFileReader(d.Hex())
	if err != nil {
		return nil, handler.Errorf("get cache file: %s", err)
	}
	pieceLength := s.pieceLengthConfig.get(info.Size())
	mi, err := torlib.NewMetaInfoFromBlob(d.Hex(), f, pieceLength)
	if err != nil {
		return nil, handler.Errorf("create metainfo: %s", err)
	}
	raw, err := mi.Serialize()
	if err != nil {
		return nil, handler.Errorf("serialize metainfo: %s", err)
	}
	return raw, nil
}

// parseDigest parses a digest from a url path parameter, e.g. "/blobs/:digest".
func parseDigest(r *http.Request) (digest image.Digest, err error) {
	d := chi.URLParam(r, "digest")
	if len(d) == 0 {
		return digest, handler.Errorf("empty digest").Status(http.StatusBadRequest)
	}
	digestRaw, err := url.PathUnescape(d)
	if err != nil {
		return digest, handler.Errorf(
			"cannot unescape digest %q: %s", d, err).Status(http.StatusBadRequest)
	}
	digest, err = image.NewDigestFromString(digestRaw)
	if err != nil {
		return digest, handler.Errorf(
			"cannot parse digest %q: %s", digestRaw, err).Status(http.StatusBadRequest)
	}
	return digest, nil
}

// parseUUID parses a uuid from a url path parameter, e.g. "/uploads/:uuid".
func parseUUID(r *http.Request) (string, error) {
	u := chi.URLParam(r, "uuid")
	if len(u) == 0 {
		return "", handler.Errorf("empty uuid").Status(http.StatusBadRequest)
	}
	if _, err := uuid.Parse(u); err != nil {
		return "", handler.Errorf("cannot parse uuid %q: %s", u, err).Status(http.StatusBadRequest)
	}
	return u, nil
}

func parseContentRange(h http.Header) (start, end int64, err error) {
	contentRange := h.Get("Content-Range")
	if len(contentRange) == 0 {
		return 0, 0, handler.Errorf("no Content-Range header").Status(http.StatusBadRequest)
	}
	parts := strings.Split(contentRange, "-")
	if len(parts) != 2 {
		return 0, 0, handler.Errorf(
			"cannot parse Content-Range header %q: expected format \"start-end\"", contentRange).
			Status(http.StatusBadRequest)
	}
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, handler.Errorf(
			"cannot parse start of range in Content-Range header %q: %s", contentRange, err).
			Status(http.StatusBadRequest)
	}
	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, handler.Errorf(
			"cannot parse end of range in Content-Range header %q: %s", contentRange, err).
			Status(http.StatusBadRequest)
	}
	// Note, no need to check for negative because the "-" would cause the
	// Split check to fail.
	return start, end, nil
}

func (s Server) getLocations(d image.Digest) ([]string, error) {
	nodes, err := s.hashState.GetOrderedNodes(d.ShardID(), s.config.NumReplica)
	if err != nil || len(nodes) == 0 {
		return nil, handler.Errorf("failed to calculate hash for digest %q: %s", d, err)
	}
	var locs []string
	for _, node := range nodes {
		locs = append(locs, s.labelToAddr[node.Label])
	}
	sort.Strings(locs)
	return locs, nil
}

func (s Server) redirectByDigest(d image.Digest) error {
	locs, err := s.getLocations(d)
	if err != nil {
		return err
	}
	for _, loc := range locs {
		if s.addr == loc {
			// Current node is among designated nodes.
			return nil
		}
	}
	return handler.Errorf("redirecting to correct nodes").
		Status(http.StatusTemporaryRedirect).
		Header("Origin-Locations", strings.Join(locs, ","))
}

func (s Server) ensureDigestExists(d image.Digest) error {
	if _, err := s.fileStore.GetCacheFileStat(d.Hex()); err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("failed to look up blob data for digest %q: %s", d, err)
	}
	return nil
}

func (s Server) ensureDigestNotExists(d image.Digest) error {
	_, err := s.fileStore.GetCacheFileStat(d.Hex())
	if err == nil {
		return handler.Errorf("digest %q already exists", d).Status(http.StatusConflict)
	}
	if err != nil && !os.IsNotExist(err) {
		return handler.Errorf("failed to look up blob data for digest %q: %s", d, err)
	}
	return nil
}

func (s Server) downloadBlob(d image.Digest, w http.ResponseWriter) error {
	f, err := s.fileStore.GetCacheFileReader(d.Hex())
	if os.IsNotExist(err) {
		return handler.ErrorStatus(http.StatusNotFound)
	} else if err != nil {
		return handler.Errorf("cannot read blob data for digest %q: %s", d, err)
	}
	defer f.Close()

	for {
		_, err := io.CopyN(w, f, int64(_uploadChunkSize))
		if err == io.EOF {
			break
		} else if err != nil {
			return handler.Errorf("cannot read digest %q: %s", d, err)
		}
	}

	return nil
}

func (s Server) deleteBlob(d image.Digest) error {
	if err := s.fileStore.MoveCacheFileToTrash(d.Hex()); err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("cannot delete blob data for digest %q: %s", d, err)
	}
	return nil
}

func (s Server) createUpload(d image.Digest) (string, error) {
	uploadUUID := uuid.Generate().String()
	if err := s.fileStore.CreateUploadFile(uploadUUID, 0); err != nil {
		return "", handler.Errorf("failed to create upload file for digest %q: %s", d, err)
	}
	return uploadUUID, nil
}

func (s Server) uploadBlobChunk(uploadUUID string, b io.ReadCloser, start, end int64) error {
	// TODO(yiran): Calculate SHA256 on the fly using https://github.com/stevvooe/resumable
	f, err := s.fileStore.GetUploadFileReadWriter(uploadUUID)
	if err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("cannot get reader for upload %q: %s", uploadUUID, err)
	}
	defer f.Close()
	if _, err := f.Seek(start, 0); err != nil {
		return handler.Errorf(
			"cannot continue upload for %q from offset %d: %s", uploadUUID, start, err).
			Status(http.StatusBadRequest)
	}
	defer b.Close()
	n, err := io.Copy(f, b)
	if err != nil {
		return handler.Errorf("failed to upload %q: %s", uploadUUID, err)
	}
	expected := end - start
	if n != expected {
		return handler.Errorf(
			"upload data length for %q doesn't match content range: got %d, expected %d",
			uploadUUID, n, expected).
			Status(http.StatusBadRequest)
	}
	return nil
}

func (s Server) commitUpload(d image.Digest, uploadUUID string) error {
	// Verify hash.
	digester := image.NewDigester()
	f, err := s.fileStore.GetUploadFileReader(uploadUUID)
	if err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("cannot get reader for upload %q: %s", uploadUUID, err)
	}
	computedDigest, err := digester.FromReader(f)
	if err != nil {
		return handler.Errorf("failed to calculate digest for upload %q: %s", uploadUUID, err)
	}
	if computedDigest != d {
		return handler.Errorf("computed digest %q doesn't match parameter %q", computedDigest, d).
			Status(http.StatusBadRequest)
	}

	// Commit data.
	if err := s.fileStore.MoveUploadFileToCache(uploadUUID, d.Hex()); err != nil {
		if os.IsExist(err) {
			return handler.Errorf("digest %q already exists", d).Status(http.StatusConflict)
		}
		return handler.Errorf("failed to commit digest %q for upload %q: %s", d, uploadUUID, err)
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

func setUploadLocation(w http.ResponseWriter, uploadUUID string) {
	w.Header().Set("Location", fmt.Sprintf(uploadUUID))
}

func setContentLength(w http.ResponseWriter, n int) {
	w.Header().Set("Content-Length", strconv.Itoa(n))
}

func setOctetStreamContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/octet-stream-v1")
}
