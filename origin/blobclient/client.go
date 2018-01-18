package blobclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// Client errors.
var (
	ErrBlobExist = errors.New("blob already exists")
)

// Client provides a wrapper around all Server HTTP endpoints.
type Client interface {
	Addr() string

	Locations(d image.Digest) ([]string, error)
	CheckBlob(d image.Digest) (bool, error)
	GetBlob(d image.Digest) (io.ReadCloser, error)
	PushBlob(d image.Digest, blob io.Reader) error
	DeleteBlob(d image.Digest) error

	Repair() (io.ReadCloser, error)
	RepairShard(shardID string) (io.ReadCloser, error)
	RepairDigest(d image.Digest) (io.ReadCloser, error)

	GetMetaInfo(namespace string, d image.Digest) (*torlib.MetaInfo, error)
	OverwriteMetaInfo(d image.Digest, pieceLength int64) error

	UploadBlob(namespace string, d image.Digest, blob io.Reader, through bool) error

	GetPeerContext() (peercontext.PeerContext, error)
}

// HTTPClient defines the Client implementation.
type HTTPClient struct {
	addr string
}

// New returns a new HTTPClient scoped to addr.
func New(addr string) *HTTPClient {
	return &HTTPClient{addr}
}

// Addr returns the address of the server the client is provisioned for.
func (c *HTTPClient) Addr() string {
	return c.addr
}

// Locations returns the origin server addresses which d is sharded on.
func (c *HTTPClient) Locations(d image.Digest) ([]string, error) {
	r, err := httputil.Get(fmt.Sprintf("http://%s/blobs/%s/locations", c.addr, d))
	if err != nil {
		return nil, err
	}
	locs := strings.Split(r.Header.Get("Origin-Locations"), ",")
	return locs, nil
}

// CheckBlob returns error if the origin does not have a blob for d.
func (c *HTTPClient) CheckBlob(d image.Digest) (bool, error) {
	_, err := httputil.Head(fmt.Sprintf("http://%s/internal/blobs/%s", c.addr, d))
	if err != nil {
		if httputil.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// GetBlob returns the blob corresponding to d.
func (c *HTTPClient) GetBlob(d image.Digest) (io.ReadCloser, error) {
	r, err := httputil.Get(fmt.Sprintf("http://%s/internal/blobs/%s", c.addr, d))
	if err != nil {
		return ioutil.NopCloser(bytes.NewReader([]byte{})), err
	}
	return r.Body, nil
}

// PushBlob uploads a blob to a single origin server. Unlike it's cousin UploadBlob,
// PushBlob is an internal API which does not replicate the blob.
//
// NOTE: Because the blob is supplied as the body of an HTTP request, if the
// underlying value of blob implements io.Closer, it will be closed.
func (c *HTTPClient) PushBlob(d image.Digest, blob io.Reader) error {
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/internal/blobs/%s/uploads", c.addr, d),
		httputil.SendBody(blob))
	return err
}

// DeleteBlob deletes the blob corresponding to d.
func (c *HTTPClient) DeleteBlob(d image.Digest) error {
	_, err := httputil.Delete(
		fmt.Sprintf("http://%s/internal/blobs/%s", c.addr, d),
		httputil.SendAcceptedCodes(http.StatusAccepted))
	return err
}

// UploadBlob uploads and replicates blob to the origin cluster. If through is set,
// UploadBlob will also upload blob to the storage backend configured for namespace.
//
// NOTE: Because the blob is supplied as the body of an HTTP request, if the
// underlying value of blob implements io.Closer, it will be closed.
func (c *HTTPClient) UploadBlob(
	namespace string, d image.Digest, blob io.Reader, through bool) error {

	_, err := httputil.Post(
		fmt.Sprintf("http://%s/namespace/%s/blobs/%s/uploads?through=%t", c.addr, namespace, d, through),
		httputil.SendBody(blob))
	return err
}

// Repair runs a global repair of all shards present on disk. See RepairShard
// for more details.
func (c *HTTPClient) Repair() (io.ReadCloser, error) {
	r, err := httputil.Post(fmt.Sprintf("http://%s/internal/repair", c.addr))
	if err != nil {
		return ioutil.NopCloser(bytes.NewReader([]byte{})), err
	}
	return r.Body, nil
}

// RepairShard pushes the blobs of shardID to other replicas, and removes shardID
// from the target origin if it is now longer an owner of shardID.
func (c *HTTPClient) RepairShard(shardID string) (io.ReadCloser, error) {
	r, err := httputil.Post(fmt.Sprintf("http://%s/internal/repair/shard/%s", c.addr, shardID))
	if err != nil {
		return ioutil.NopCloser(bytes.NewReader([]byte{})), err
	}
	return r.Body, nil
}

// RepairDigest pushes d to other replicas, and removes d from the target origin
// if it is no longer the owner of d.
func (c *HTTPClient) RepairDigest(d image.Digest) (io.ReadCloser, error) {
	r, err := httputil.Post(fmt.Sprintf("http://%s/internal/repair/digest/%s", c.addr, d))
	if err != nil {
		return ioutil.NopCloser(bytes.NewReader([]byte{})), err
	}
	return r.Body, nil
}

// GetMetaInfo returns metainfo for d. If the blob of d is not available yet
// (i.e. still downloading), returns a 202 httputil.StatusError, indicating that
// the request should be retried later. If no blob exists for d, returns a 404
// httputil.StatusError.
func (c *HTTPClient) GetMetaInfo(namespace string, d image.Digest) (*torlib.MetaInfo, error) {
	r, err := httputil.Get(fmt.Sprintf(
		"http://%s/internal/namespace/%s/blobs/%s/metainfo", c.addr, namespace, d))
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	raw, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %s", err)
	}
	mi, err := torlib.DeserializeMetaInfo(raw)
	if err != nil {
		return nil, fmt.Errorf("deserialize metainfo: %s", err)
	}
	return mi, nil
}

// OverwriteMetaInfo overwrites existing metainfo for d with new metainfo
// configured with pieceLength. Primarily intended for benchmarking purposes.
func (c *HTTPClient) OverwriteMetaInfo(d image.Digest, pieceLength int64) error {
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/internal/blobs/%s/metainfo?piece_length=%d", c.addr, d, pieceLength))
	return err
}

// GetPeerContext gets the PeerContext of the p2p client running alongside the Server.
func (c *HTTPClient) GetPeerContext() (peercontext.PeerContext, error) {
	var pctx peercontext.PeerContext
	r, err := httputil.Get(fmt.Sprintf("http://%s/internal/peercontext", c.addr))
	if err != nil {
		return pctx, err
	}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&pctx); err != nil {
		return pctx, err
	}
	return pctx, nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
