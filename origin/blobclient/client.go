// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package blobclient

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/memsize"
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

	ForceCleanup(ttl time.Duration) error
}

// HTTPClient defines the Client implementation.
type HTTPClient struct {
	addr      string
	chunkSize uint64
	tls       *tls.Config
}

// Option allows setting optional HTTPClient parameters.
type Option func(*HTTPClient)

// WithChunkSize configures an HTTPClient with a custom chunk size for uploads.
func WithChunkSize(s uint64) Option {
	return func(c *HTTPClient) { c.chunkSize = s }
}

// WithTLS configures an HTTPClient with tls configuration.
func WithTLS(tls *tls.Config) Option {
	return func(c *HTTPClient) { c.tls = tls }
}

// New returns a new HTTPClient scoped to addr.
func New(addr string, opts ...Option) *HTTPClient {
	c := &HTTPClient{
		addr:      addr,
		chunkSize: 32 * memsize.MB,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Addr returns the address of the server the client is provisioned for.
func (c *HTTPClient) Addr() string {
	return c.addr
}

// Locations returns the origin server addresses which d is sharded on.
func (c *HTTPClient) Locations(d core.Digest) ([]string, error) {
	r, err := httputil.Get(
		fmt.Sprintf("http://%s/blobs/%s/locations", c.addr, d),
		httputil.SendTimeout(5*time.Second),
		httputil.SendTLS(c.tls))
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

	r, err := httputil.Head(
		u,
		httputil.SendTimeout(15*time.Second),
		httputil.SendTLS(c.tls))
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
		httputil.SendAcceptedCodes(http.StatusAccepted),
		httputil.SendTLS(c.tls))
	return err
}

// TransferBlob uploads a blob to a single origin server. Unlike its cousin UploadBlob,
// TransferBlob is an internal API which does not replicate the blob.
func (c *HTTPClient) TransferBlob(d core.Digest, blob io.Reader) error {
	tc := newTransferClient(c.addr, c.tls)
	return runChunkedUpload(tc, d, blob, int64(c.chunkSize))
}

// UploadBlob uploads and replicates blob to the origin cluster, asynchronously
// backing the blob up to the remote storage configured for namespace.
func (c *HTTPClient) UploadBlob(namespace string, d core.Digest, blob io.Reader) error {
	uc := newUploadClient(c.addr, namespace, _publicUpload, 0, c.tls)
	return runChunkedUpload(uc, d, blob, int64(c.chunkSize))
}

// DuplicateUploadBlob duplicates an blob upload request, which will attempt to
// write-back at the given delay.
func (c *HTTPClient) DuplicateUploadBlob(
	namespace string, d core.Digest, blob io.Reader, delay time.Duration) error {

	uc := newUploadClient(c.addr, namespace, _duplicateUpload, delay, c.tls)
	return runChunkedUpload(uc, d, blob, int64(c.chunkSize))
}

// DownloadBlob downloads blob for d. If the blob of d is not available yet
// (i.e. still downloading), returns 202 httputil.StatusError, indicating that
// the request shoudl be retried later. If not blob exists for d, returns a 404
// httputil.StatusError.
func (c *HTTPClient) DownloadBlob(namespace string, d core.Digest, dst io.Writer) error {
	r, err := httputil.Get(
		fmt.Sprintf("http://%s/namespace/%s/blobs/%s", c.addr, url.PathEscape(namespace), d),
		httputil.SendTLS(c.tls))
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
			c.addr, url.PathEscape(namespace), d, remoteDNS),
		httputil.SendTLS(c.tls))
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
		httputil.SendTimeout(15*time.Second),
		httputil.SendTLS(c.tls))
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
		fmt.Sprintf("http://%s/internal/blobs/%s/metainfo?piece_length=%d", c.addr, d, pieceLength),
		httputil.SendTLS(c.tls))
	return err
}

// GetPeerContext gets the PeerContext of the p2p client running alongside the Server.
func (c *HTTPClient) GetPeerContext() (core.PeerContext, error) {
	var pctx core.PeerContext
	r, err := httputil.Get(
		fmt.Sprintf("http://%s/internal/peercontext", c.addr),
		httputil.SendTimeout(5*time.Second),
		httputil.SendTLS(c.tls))
	if err != nil {
		return pctx, err
	}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&pctx); err != nil {
		return pctx, err
	}
	return pctx, nil
}

// ForceCleanup forces cache cleanup to run.
func (c *HTTPClient) ForceCleanup(ttl time.Duration) error {
	v := url.Values{}
	v.Add("ttl_hr", strconv.Itoa(int(math.Ceil(float64(ttl)/float64(time.Hour)))))
	_, err := httputil.Post(
		fmt.Sprintf("http://%s/forcecleanup?%s", c.addr, v.Encode()),
		httputil.SendTimeout(2*time.Minute),
		httputil.SendTLS(c.tls))
	return err
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
