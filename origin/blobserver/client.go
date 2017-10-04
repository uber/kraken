package blobserver

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/utils/httputil"
)

// RedirectError occurs when a Client is scoped to the wrong origin.
type RedirectError struct {
	Locations []string
}

func newRedirectError(h http.Header) RedirectError {
	locations := strings.Split(h.Get("Origin-Locations"), ",")
	return RedirectError{locations}
}

func (e RedirectError) Error() string {
	return fmt.Sprintf("incorrect origin, must redirect request to: %v", e.Locations)
}

// Client provides a wrapper around all Server HTTP endpoints.
type Client interface {
	CheckBlob(d image.Digest) error
	GetBlob(d image.Digest) (io.ReadCloser, error)
	DeleteBlob(d image.Digest) error
	UploadBlob(d image.Digest) (uuid string, err error)
	PatchUpload(d image.Digest, uuid string, start, stop int64, chunk io.Reader) error
	CommitUpload(d image.Digest, uuid string) error
	Repair() (io.ReadCloser, error)
	RepairShard(shardID string) (io.ReadCloser, error)
	RepairDigest(d image.Digest) (io.ReadCloser, error)
}

var _ Client = (*HTTPClient)(nil)

// HTTPClient defines the Client implementation.
type HTTPClient struct {
	addr string
}

// NewHTTPClient returns a new HTTPClient scoped to addr.
func NewHTTPClient(addr string) *HTTPClient {
	return &HTTPClient{addr}
}

// CheckBlob returns error if the origin does not have a blob for d.
func (c *HTTPClient) CheckBlob(d image.Digest) error {
	_, err := httputil.Head(fmt.Sprintf("http://%s/blobs/%s", c.addr, d))
	return maybeRedirect(err)
}

// GetBlob returns the blob corresponding to d.
func (c *HTTPClient) GetBlob(d image.Digest) (io.ReadCloser, error) {
	r, err := httputil.Get(fmt.Sprintf("http://%s/blobs/%s", c.addr, d))
	if err != nil {
		return nil, maybeRedirect(err)
	}
	return r.Body, nil
}

// DeleteBlob deletes the blob corresponding to d.
func (c *HTTPClient) DeleteBlob(d image.Digest) error {
	_, err := httputil.Delete(
		fmt.Sprintf("http://%s/blobs/%s", c.addr, d),
		httputil.SendAcceptedCodes(http.StatusAccepted))
	return maybeRedirect(err)
}

// UploadBlob marks d as ready for upload, returning the upload uuid to use for
// future patches and commit.
func (c *HTTPClient) UploadBlob(d image.Digest) (uuid string, err error) {
	r, err := httputil.Post(
		fmt.Sprintf("http://%s/blobs/%s/uploads", c.addr, d),
		httputil.SendAcceptedCodes(http.StatusAccepted))
	if err != nil {
		return "", maybeRedirect(err)
	}
	uuid = r.Header.Get("Location")
	if uuid == "" {
		return "", errors.New("request succeeded, but Location header not set")
	}
	return uuid, nil
}

// PatchUpload uploads a chunk of d's blob from start to stop byte indeces for
// the upload of uuid.
func (c *HTTPClient) PatchUpload(d image.Digest, uuid string, start, stop int64, chunk io.Reader) error {
	_, err := httputil.Patch(
		fmt.Sprintf("http://%s/blobs/%s/uploads/%s", c.addr, d, uuid),
		httputil.SendBody(chunk),
		httputil.SendHeaders(map[string]string{
			"Content-Range": fmt.Sprintf("%d-%d", start, stop),
		}),
		httputil.SendAcceptedCodes(http.StatusAccepted))
	return maybeRedirect(err)
}

// CommitUpload marks the upload uuid for d's blob as committed.
func (c *HTTPClient) CommitUpload(d image.Digest, uuid string) error {
	_, err := httputil.Put(
		fmt.Sprintf("http://%s/blobs/%s/uploads/%s", c.addr, d, uuid),
		httputil.SendAcceptedCodes(http.StatusCreated))
	return maybeRedirect(err)
}

// Repair TODO
func (c *HTTPClient) Repair() (io.ReadCloser, error) {
	panic("repair not implemented")
}

// RepairShard TODO
func (c *HTTPClient) RepairShard(shardID string) (io.ReadCloser, error) {
	panic("repair shard not implemented")
}

// RepairDigest TODO
func (c *HTTPClient) RepairDigest(d image.Digest) (io.ReadCloser, error) {
	panic("repair digest not implemented")
}

// maybeRedirect attempts to convert redirects into RedirectErrors.
func maybeRedirect(err error) error {
	if err == nil {
		return nil
	}
	if serr, ok := err.(httputil.StatusError); ok && serr.Status == http.StatusTemporaryRedirect {
		return newRedirectError(serr.Header)
	}
	return err
}
