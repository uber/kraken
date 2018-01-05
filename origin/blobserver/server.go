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

	rc := dedup.NewRequestCache(config.RequestCache, clock.New())
	rc.SetNotFound(func(err error) bool { return err == backend.ErrBlobNotFound })

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
		requestCache:      rc,
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

		r.Post("/blobs/:digest/uploads", handler.Wrap(s.pushBlobHandler))
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
		stats := s.stats.SubScope("namespace.blobs.uploads")
		r.Use(middleware.Counter(stats))
		r.Use(middleware.ElapsedTimer(stats))

		r.Post("/namespace/:namespace/blobs/:digest/uploads", handler.Wrap(s.uploadBlobHandler))
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
	if ok, err := s.blobExists(d); err != nil {
		return err
	} else if !ok {
		return handler.ErrorStatus(http.StatusNotFound)
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

// pushBlobHandler accepts a blob via chunked transfer encoding.
func (s Server) pushBlobHandler(w http.ResponseWriter, r *http.Request) error {
	defer r.Body.Close()
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	if err := s.redirectByDigest(d); err != nil {
		return err
	}
	if exists, err := s.blobExists(d); err != nil {
		return err
	} else if exists {
		return nil
	}
	return s.downloadPushedBlob(d, r.Body)
}

// uploadBlobHandler uploads a blob via chunked transfer encoding. Replicates
// the blob among other origins. If query argument "through" is set to true,
// pushes the blob to the storage backend configured for the given namespace.
func (s Server) uploadBlobHandler(w http.ResponseWriter, r *http.Request) error {
	defer r.Body.Close()
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	if err := s.redirectByDigest(d); err != nil {
		return err
	}
	namespace := chi.URLParam(r, "namespace")
	if namespace == "" {
		return handler.Errorf("empty namespace").Status(http.StatusBadRequest)
	}
	through := false
	t := r.URL.Query().Get("through")
	if t != "" {
		through, err = strconv.ParseBool(t)
		if err != nil {
			return handler.Errorf(
				"invalid through argument: parse bool: %s", err).Status(http.StatusBadRequest)
		}
	}
	return s.uploadBlob(namespace, d, r.Body, through)
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
			return err
		}
		if err := s.replicateBlob(d); err != nil {
			// Don't return error here as we only want to cache storage backend errors.
			log.With("blob", d.Hex()).Errorf("Error replicating remote blob: %s", err)
		}
		return nil
	})
	if err == dedup.ErrRequestPending || err == nil {
		return handler.ErrorStatus(http.StatusAccepted)
	} else if err == backend.ErrBlobNotFound {
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
	f, err := s.fileStore.GetCacheFileReader(d.Hex())
	if err != nil {
		return handler.Errorf("get cache reader: %s", err)
	}
	if err := s.clientProvider.Provide(loc).PushBlob(d, f); err != nil {
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

func (s Server) blobExists(d image.Digest) (bool, error) {
	if _, err := s.fileStore.GetCacheFileStat(d.Hex()); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, handler.Errorf("cache file stat: %s", err)
	}
	return true, nil
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

func (s Server) downloadPushedBlob(d image.Digest, blob io.Reader) error {
	u := uuid.Generate().String()
	if err := s.fileStore.CreateUploadFile(u, 0); err != nil {
		return handler.Errorf("create upload file: %s", err)
	}
	f, err := s.fileStore.GetUploadFileReadWriter(u)
	if err != nil {
		return handler.Errorf("get upload file: %s", err)
	}
	digester := image.NewDigester()
	if _, err := io.Copy(f, digester.Tee(blob)); err != nil {
		return handler.Errorf("copy blob: %s", err)
	}
	resd := digester.Digest()
	if resd != d {
		return handler.Errorf(
			"expected digest %s, calculated %s from blob", d, resd).Status(http.StatusBadRequest)
	}
	if err := s.fileStore.MoveUploadFileToCache(u, d.Hex()); err != nil {
		return handler.Errorf("move upload file to cache: %s", err)
	}
	return nil
}

func (s Server) uploadBlob(namespace string, d image.Digest, blob io.Reader, through bool) error {
	exists, err := s.blobExists(d)
	if err != nil {
		return err
	}

	// Reader for uploading blob to storage backend. May originate from either
	// uploads or cache based on whether blob already exists.
	var f store.FileReader

	var uploadFilename string
	if !exists {
		uploadFilename = uuid.Generate().String()
		if err := s.fileStore.CreateUploadFile(uploadFilename, 0); err != nil {
			return handler.Errorf("create upload file: %s", err)
		}
		rw, err := s.fileStore.GetUploadFileReadWriter(uploadFilename)
		if err != nil {
			return handler.Errorf("get upload file: %s", err)
		}
		digester := image.NewDigester()
		if _, err := io.Copy(rw, digester.Tee(blob)); err != nil {
			return handler.Errorf("copy blob: %s", err)
		}
		resd := digester.Digest()
		if resd != d {
			return handler.Errorf(
				"expected digest %s, calculated %s from blob", d, resd).Status(http.StatusBadRequest)
		}
		// Reset file for reading.
		if _, err := rw.Seek(0, 0); err != nil {
			return handler.Errorf("upload file seek: %s", err)
		}
		f = rw // Use upload file for backend upload.
	}

	// If through is set, we must make sure we safely upload the file to namespace's
	// storage backend before committing the file to the cache. If the file can't be
	// uploaded to said backend, the entire upload operation must fail.
	if through {
		if exists {
			r, err := s.fileStore.GetCacheFileReader(d.Hex())
			if err != nil {
				return handler.Errorf("get cache file: %s", err)
			}
			f = r // Use cache file for backend upload.
		}
		c, err := s.backendManager.GetClient(namespace)
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

	if !exists {
		if err := s.fileStore.MoveUploadFileToCache(uploadFilename, d.Hex()); err != nil {
			return handler.Errorf("move upload file to cache: %s", err)
		}
		if err := s.replicateBlob(d); err != nil {
			log.With("blob", d.Hex()).Errorf("Error replicating uploaded blob: %s", err)
		}
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
