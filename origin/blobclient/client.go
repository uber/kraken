package blobclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/httputil"

	"github.com/c2h5oh/datasize"
)

// Client provides a wrapper around all Server HTTP endpoints.
type Client interface {
	Addr() string

	Locations(d core.Digest) ([]string, error)
	DeleteBlob(d core.Digest) error
	TransferBlob(d core.Digest, blob io.Reader) error

	Stat(namespace string, d core.Digest) (*core.BlobInfo, error)
	StatLocal(namespace string, d core.Digest) (*core.BlobInfo, error)

	GetMetaInfo(namespace string, d core.Digest) (*core.MetaInfo, error)
	OverwriteMetaInfo(d core.Digest, pieceLength int64) error

	UploadBlob(namespace string, d core.Digest, blob io.Reader) error
	DuplicateUploadBlob(namespace string, d core.Digest, blob io.Reader, delay time.Duration) error

	DownloadBlob(namespace string, d core.Digest, dst io.Writer) error

	ReplicateToRemote(namespace string, d core.Digest, remoteDNS string) error

	GetPeerContext() (core.PeerContext, error)
}

// Config defines HTTPClient configuration.
type Config struct {
	ChunkSize datasize.ByteSize `yaml:"chunk_size"`
}

func (c Config) applyDefaults() Config {
	if c.ChunkSize == 0 {
		c.ChunkSize = 32 * datasize.MB
	}
	return c
}

// HTTPClient defines the Client implementation.
type HTTPClient struct {
	addr   string
	config Config
}

// New returns a new HTTPClient scoped to addr with default config.
func New(addr string) *HTTPClient {
	return NewWithConfig(addr, Config{})
}

// NewWithConfig returns a new HTTPClient scoped to addr with config.
func NewWithConfig(addr string, config Config) *HTTPClient {
	config = config.applyDefaults()
	return &HTTPClient{addr, config}
}

// Addr returns the address of the server the client is provisioned for.
func (c *HTTPClient) Addr() string {
	return c.addr
}

// Locations returns the origin server addresses which d is sharded on.
func (c *HTTPClient) Locations(d core.Digest) ([]string, error) {
	r, err := httputil.Get(
		fmt.Sprintf("http://%s/blobs/%s/locations", c.addr, d),
		httputil.SendRetry(),
		httputil.SendTimeout(5*time.Second))
	if err != nil {
		return nil, err
	}
	locs := strings.Split(r.Header.Get("Origin-Locations"), ",")
	if len(locs) == 0 {
		return nil, errors.New("no locations found")
	}
	return locs, nil
}

// Stat returns blob info. It returns error if the origin does not have a blob
// for d.
func (c *HTTPClient) Stat(namespace string, d core.Digest) (*core.BlobInfo, error) {
	return c.stat(namespace, d, false)
}

// StatLocal returns blob info. It returns error if the origin does not have a blob
// for d locally.
func (c *HTTPClient) StatLocal(namespace string, d core.Digest) (*core.BlobInfo, error) {
	return c.stat(namespace, d, true)
}

func (c *HTTPClient) stat(namespace string, d core.Digest, local bool) (*core.BlobInfo, error) {
	u := fmt.Sprintf(
		"http://%s/internal/namespace/%s/blobs/%s",
		c.addr,
		url.PathEscape(namespace),
		d)
	if local {
		u += "?local=true"
	}

	r, err := httputil.Head(u, httputil.SendTimeout(15*time.Second))
	if err != nil {
		if httputil.IsNotFound(err) {
			return nil, ErrBlobNotFound
		}
		return nil, err
	}
	var size int64
	hdr := r.Header.Get("Content-Length")
	if hdr != "" {
		size, err = strconv.ParseInt(hdr, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	return core.NewBlobInfo(size), nil
}

// DeleteBlob deletes the blob corresponding to d.
func (c *HTTPClient) DeleteBlob(d core.Digest) error {
	_, err := httputil.Delete(
		fmt.Sprintf("http://%s/internal/blobs/%s", c.addr, d),
		httputil.SendAcceptedCodes(http.StatusAccepted))
	return err
}

// TransferBlob uploads a blob to a single origin server. Unlike its cousin UploadBlob,
// TransferBlob is an internal API which does not replicate the blob.
func (c *HTTPClient) TransferBlob(d core.Digest, blob io.Reader) error {
	tc := newTransferClient(c.addr)
	return runChunkedUpload(tc, d, blob, int64(c.config.ChunkSize))
}

// UploadBlob uploads and replicates blob to the origin cluster, asynchronously
// backing the blob up to the remote storage configured for namespace.
func (c *HTTPClient) UploadBlob(namespace string, d core.Digest, blob io.Reader) error {
	uc := newUploadClient(c.addr, namespace, _publicUpload, 0)
	return runChunkedUpload(uc, d, blob, int64(c.config.ChunkSize))
}

// DuplicateUploadBlob duplicates an blob upload request, which will attempt to
// write-back at the given delay.
func (c *HTTPClient) DuplicateUploadBlob(
	namespace string, d core.Digest, blob io.Reader, delay time.Duration) error {

	uc := newUploadClient(c.addr, namespace, _duplicateUpload, delay)
	return runChunkedUpload(uc, d, blob, int64(c.config.ChunkSize))
}

// DownloadBlob downloads blob for d. If the blob of d is not available yet
// (i.e. still downloading), returns 202 httputil.StatusError, indicating that
// the request shoudl be retried later. If not blob exists for d, returns a 404
// httputil.StatusError.
func (c *HTTPClient) DownloadBlob(namespace string, d core.Digest, dst io.Writer) error {
	r, err := httputil.Get(
		fmt.Sprintf("http://%s/namespace/%s/blobs/%s", c.addr, url.PathEscape(namespace), d))
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if _, err := io.Copy(dst, r.Body); err != nil {
		return fmt.Errorf("copy body: %s", err)
	}
	return nil
}

// ReplicateToRemote replicates the blob of d to a remote origin cluster. If the
// blob of d is not available yet, returns 202 httputil.StatusError, indicating
// that the request should be retried later.
func (c *HTTPClient) ReplicateToRemote(namespace string, d core.Digest, remoteDNS string) error {
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/namespace/%s/blobs/%s/remote/%s",
			c.addr, url.PathEscape(namespace), d, remoteDNS))
	return err
}

// GetMetaInfo returns metainfo for d. If the blob of d is not available yet
// (i.e. still downloading), returns a 202 httputil.StatusError, indicating that
// the request should be retried later. If no blob exists for d, returns a 404
// httputil.StatusError.
func (c *HTTPClient) GetMetaInfo(namespace string, d core.Digest) (*core.MetaInfo, error) {
	r, err := httputil.Get(
		fmt.Sprintf("http://%s/internal/namespace/%s/blobs/%s/metainfo",
			c.addr, url.PathEscape(namespace), d),
		httputil.SendTimeout(15*time.Second))
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	raw, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %s", err)
	}
	mi, err := core.DeserializeMetaInfo(raw)
	if err != nil {
		return nil, fmt.Errorf("deserialize metainfo: %s", err)
	}
	return mi, nil
}

// OverwriteMetaInfo overwrites existing metainfo for d with new metainfo
// configured with pieceLength. Primarily intended for benchmarking purposes.
func (c *HTTPClient) OverwriteMetaInfo(d core.Digest, pieceLength int64) error {
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/internal/blobs/%s/metainfo?piece_length=%d", c.addr, d, pieceLength))
	return err
}

// GetPeerContext gets the PeerContext of the p2p client running alongside the Server.
func (c *HTTPClient) GetPeerContext() (core.PeerContext, error) {
	var pctx core.PeerContext
	r, err := httputil.Get(
		fmt.Sprintf("http://%s/internal/peercontext", c.addr),
		httputil.SendTimeout(5*time.Second))
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
