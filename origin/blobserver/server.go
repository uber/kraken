package blobserver

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/client"
	"code.uber.internal/infra/kraken/utils/memsize"

	"github.com/docker/distribution/uuid"
	"github.com/pressly/chi"
	"github.com/spaolacci/murmur3"
)

const _uploadChunkSize = 16 * memsize.MB

// Server defines a server that serves blob data for agent.
type Server struct {
	config              Config
	label               string
	hostname            string
	labelToHostname     map[string]string
	hashState           *hrw.RendezvousHash
	fileStore           store.FileStore
	blobTransferFactory client.BlobTransferFactory
}

// New initializes a new Server.
func New(
	config Config,
	hostname string,
	fileStore store.FileStore,
	blobTransferFactory client.BlobTransferFactory) (*Server, error) {

	if len(config.HashNodes) == 0 {
		return nil, errors.New("no hash nodes configured")
	}

	hashState := hrw.NewRendezvousHash(
		func() hash.Hash { return murmur3.New64() },
		hrw.UInt64ToFloat64)
	labelToHostname := make(map[string]string, len(config.HashNodes))
	for h, node := range config.HashNodes {
		hashState.AddNode(node.Label, node.Weight)
		labelToHostname[node.Label] = h
	}

	currNode, ok := config.HashNodes[hostname]
	if !ok {
		return nil, fmt.Errorf("hostname %q not in configured hash nodes", hostname)
	}
	label := currNode.Label

	return &Server{
		config:              config,
		label:               label,
		hostname:            hostname,
		labelToHostname:     labelToHostname,
		hashState:           hashState,
		fileStore:           fileStore,
		blobTransferFactory: client.NewBlobAPIClient,
	}, nil
}

type errHandler func(http.ResponseWriter, *http.Request) error

// handler converts an errHandler into a proper http.HandlerFunc. This allows
// handlers of Server to return errors without worrying about applying the
// error to the response.
func handler(h errHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			switch e := err.(type) {
			case *serverError:
				for k, vs := range e.header {
					for _, v := range vs {
						w.Header().Add(k, v)
					}
				}
				w.WriteHeader(e.status)
				w.Write([]byte(e.msg))
			default:
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(e.Error()))
			}
		}
	}
}

// Handler returns an http handler for the blob server.
func (s Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Head("/blobs/:digest", handler(s.checkBlobHandler))
	r.Get("/blobs/:digest", handler(s.getBlobHandler))
	r.Delete("/blobs/:digest", handler(s.deleteBlobHandler))

	r.Post("/blobs/:digest/uploads", handler(s.startUploadHandler))
	r.Patch("/blobs/:digest/uploads/:uuid", handler(s.patchUploadHandler))
	r.Put("/blobs/:digest/uploads/:uuid", handler(s.commitUploadHandler))

	// TODO(codyg): Revisit repair endpoints.
	r.Post("/repair/digest/:digest", handler(s.repairBlobByDigestHandler))
	r.Post("/repair/shard/:shardID", handler(s.repairBlobByShardIDHandler))

	return r
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
	return nil
}

// repairBlobByShardIDHandler runs blob repair by shard ID, ensuring all
// digests of a shard are synced properly to the target nodes.
func (s Server) repairBlobByShardIDHandler(w http.ResponseWriter, r *http.Request) error {
	shardID := chi.URLParam(r, "shardID")
	if len(shardID) == 0 {
		return serverErrorf("empty shard id").Status(http.StatusBadRequest)
	}
	digests, err := s.getDigests(shardID)
	if err != nil {
		return err
	}
	nodes, err := s.hashState.GetOrderedNodes(shardID, s.config.NumReplica)
	if err != nil {
		return serverErrorf("failed to compute nodes of shard %q: %s", shardID, err)
	}
	if err := s.batchRepair(nodes, digests, w); err != nil {
		return err
	}
	return nil
}

// repairBlobByDigestHandler runs blob repair for a digest, ensuring said
// digest is synced properly to the target nodes.
func (s Server) repairBlobByDigestHandler(w http.ResponseWriter, r *http.Request) error {
	d, err := parseDigest(r)
	if err != nil {
		return err
	}
	shardID := d.GetShardID()
	nodes, err := s.hashState.GetOrderedNodes(shardID, s.config.NumReplica)
	if err != nil {
		return serverErrorf("failed to compute nodes of shard %q: %s", shardID, err)
	}
	if err := s.batchRepair(nodes, []*image.Digest{d}, w); err != nil {
		return err
	}
	return nil
}

// parseDigest parses a digest from a url path parameter, e.g. "/blobs/:digest".
func parseDigest(r *http.Request) (*image.Digest, error) {
	d := chi.URLParam(r, "digest")
	if len(d) == 0 {
		return nil, serverErrorf("empty digest").Status(http.StatusBadRequest)
	}
	digestRaw, err := url.PathUnescape(d)
	if err != nil {
		return nil, serverErrorf(
			"cannot unescape digest %q: %s", d, err).Status(http.StatusBadRequest)
	}
	digest, err := image.NewDigestFromString(digestRaw)
	if err != nil {
		return nil, serverErrorf(
			"cannot parse digest %q: %s", digestRaw, err).Status(http.StatusBadRequest)
	}
	return digest, nil
}

// parseUUID parses a uuid from a url path parameter, e.g. "/uploads/:uuid".
func parseUUID(r *http.Request) (string, error) {
	u := chi.URLParam(r, "uuid")
	if len(u) == 0 {
		return "", serverErrorf("empty uuid").Status(http.StatusBadRequest)
	}
	if _, err := uuid.Parse(u); err != nil {
		return "", serverErrorf("cannot parse uuid %q: %s", u, err).Status(http.StatusBadRequest)
	}
	return u, nil
}

func parseContentRange(h http.Header) (start, end int64, err error) {
	contentRange := h.Get("Content-Range")
	if len(contentRange) == 0 {
		return 0, 0, serverErrorf("no Content-Range header").Status(http.StatusBadRequest)
	}
	parts := strings.Split(contentRange, "-")
	if len(parts) != 2 {
		return 0, 0, serverErrorf(
			"cannot parse Content-Range header %q: expected format \"start-end\"", contentRange).
			Status(http.StatusBadRequest)
	}
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, serverErrorf(
			"cannot parse start of range in Content-Range header %q: %s", contentRange, err).
			Status(http.StatusBadRequest)
	}
	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, serverErrorf(
			"cannot parse end of range in Content-Range header %q: %s", contentRange, err).
			Status(http.StatusBadRequest)
	}
	// Note, no need to check for negative because the "-" would cause the
	// Split check to fail.
	return start, end, nil
}

func (s Server) redirectByDigest(d *image.Digest) error {
	nodes, err := s.hashState.GetOrderedNodes(d.GetShardID(), s.config.NumReplica)
	if err != nil || len(nodes) == 0 {
		return serverErrorf("failed to calculate hash for digest %q: %s", d, err)
	}
	var labels []string
	for _, node := range nodes {
		if node.Label == s.label {
			// Current node is among the designated nodes.
			return nil
		}
		labels = append(labels, node.Label)
	}
	sort.Strings(labels)
	return serverErrorf("redirecting to correct nodes").
		Status(http.StatusTemporaryRedirect).
		Header("Origin-Locations", strings.Join(labels, ","))
}

func (s Server) ensureDigestExists(d *image.Digest) error {
	if _, err := s.fileStore.GetCacheFileStat(d.Hex()); err != nil {
		if os.IsNotExist(err) {
			return newBlobNotFoundError(d, err)
		}
		return serverErrorf("failed to look up blob data for digest %q: %s", d, err)
	}
	return nil
}

func (s Server) ensureDigestNotExists(d *image.Digest) error {
	_, err := s.fileStore.GetCacheFileStat(d.Hex())
	if err == nil {
		return serverErrorf("digest %q already exists: %s", d, err).Status(http.StatusConflict)
	}
	if err != nil && !os.IsNotExist(err) {
		return serverErrorf("failed to look up blob data for digest %q: %s", d, err)
	}
	return nil
}

func (s Server) downloadBlob(d *image.Digest, w http.ResponseWriter) error {
	f, err := s.fileStore.GetCacheFileReader(d.Hex())
	if os.IsNotExist(err) {
		return newBlobNotFoundError(d, err)
	} else if err != nil {
		return serverErrorf("cannot read blob data for digest %q: %s", d, err)
	}
	defer f.Close()

	for {
		_, err := io.CopyN(w, f, int64(_uploadChunkSize))
		if err == io.EOF {
			break
		} else if err != nil {
			return serverErrorf("cannot read digest %q: %s", d, err)
		}
	}

	return nil
}

func (s Server) deleteBlob(d *image.Digest) error {
	if err := s.fileStore.MoveCacheFileToTrash(d.Hex()); err != nil {
		if os.IsNotExist(err) {
			return newBlobNotFoundError(d, err)
		}
		return serverErrorf("cannot delete blob data for digest %q: %s", d, err)
	}
	return nil
}

func (s Server) createUpload(d *image.Digest) (string, error) {
	uploadUUID := uuid.Generate().String()
	if err := s.fileStore.CreateUploadFile(uploadUUID, 0); err != nil {
		return "", serverErrorf("failed to create upload file for digest %q: %s", d, err)
	}
	return uploadUUID, nil
}

func (s Server) uploadBlobChunk(uploadUUID string, b io.ReadCloser, start, end int64) error {
	// TODO(yiran): Calculate SHA256 on the fly using https://github.com/stevvooe/resumable
	f, err := s.fileStore.GetUploadFileReadWriter(uploadUUID)
	if err != nil {
		if os.IsNotExist(err) {
			return newUploadNotFoundError(uploadUUID, err)
		}
		return serverErrorf("cannot get reader for upload %q: %s", uploadUUID, err)
	}
	defer f.Close()
	if _, err := f.Seek(start, 0); err != nil {
		return serverErrorf(
			"cannot continue upload for %q from offset %d: %s", uploadUUID, start, err).
			Status(http.StatusBadRequest)
	}
	defer b.Close()
	n, err := io.Copy(f, b)
	if err != nil {
		return serverErrorf("failed to upload %q: %s", uploadUUID, err)
	}
	expected := end - start
	if n != expected {
		return serverErrorf(
			"upload data length for %q doesn't match content range: got %d, expected %d",
			uploadUUID, n, expected).
			Status(http.StatusBadRequest)
	}
	return nil
}

func (s Server) commitUpload(d *image.Digest, uploadUUID string) error {
	// Verify hash.
	digester := image.NewDigester()
	f, err := s.fileStore.GetUploadFileReader(uploadUUID)
	if err != nil {
		if os.IsNotExist(err) {
			return newUploadNotFoundError(uploadUUID, err)
		}
		return serverErrorf("cannot get reader for upload %q: %s", uploadUUID, err)
	}
	computedDigest, err := digester.FromReader(f)
	if err != nil {
		return serverErrorf("failed to calculate digest for upload %q: %s", uploadUUID, err)
	}
	if *computedDigest != *d {
		return serverErrorf("computed digest %q doesn't match parameter %q", computedDigest, d).
			Status(http.StatusBadRequest)
	}

	// Commit data.
	if err := s.fileStore.MoveUploadFileToCache(uploadUUID, d.Hex()); err != nil {
		return serverErrorf("failed to commit digest %q for upload %q: %s", d, uploadUUID, err)
	}

	return nil
}

func (s Server) getDigests(shardID string) ([]*image.Digest, error) {
	names, err := s.fileStore.ListCacheFilesByShardID(shardID)
	if err != nil {
		// TODO(codyg): Maybe 404 here?
		return nil, serverErrorf("failed to retrieve digests for shard %q: %s", shardID, err)
	}
	digests := make([]*image.Digest, len(names))
	for i, name := range names {
		d, err := image.NewDigestFromString("sha256:" + name)
		if err != nil {
			return nil, serverErrorf("failed to parse digest %q: %s", name, err)
		}
		digests[i] = d
	}
	return digests, nil
}

func (s Server) batchRepair(
	nodes []*hrw.RendezvousHashNode, digests []*image.Digest, w http.ResponseWriter) error {

	if len(nodes) == 0 {
		err := serverErrorf("invalid hash configuration: no nodes found")
		log.Errorf("Invariant violation: %s", err)
		return err
	}
	for _, node := range nodes {
		// Skip repair for the current node.
		if node.Label == s.label {
			continue
		}

		host, ok := s.labelToHostname[node.Label]
		if !ok {
			err := serverErrorf("cannot find server for label %q", node.Label)
			log.Errorf("Invariant violation: %s", err)
			return err
		}

		br := &BlobRepairer{
			context:  context.TODO(),
			hostname: host,
			blobAPI:  s.blobTransferFactory(host, s.fileStore),
			config:   s.config.Repair,
		}
		// Batch repairer launches a number of background go-routines
		// and reports the result back into response writer asynchronously
		br.BatchRepair(digests, w)
	}
	return nil
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
