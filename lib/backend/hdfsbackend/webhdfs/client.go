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
package webhdfs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/memsize"
)

// Client wraps webhdfs operations. All paths must be absolute.
type Client interface {
	Create(path string, src io.Reader) error
	Rename(from, to string) error
	Mkdirs(path string) error
	Open(path string, dst io.Writer) error
	GetFileStatus(path string) (FileStatus, error)
	ListFileStatus(path string) ([]FileStatus, error)
}

type allNameNodesFailedError struct {
	err error
}

func (e allNameNodesFailedError) Error() string {
	return fmt.Sprintf("all name nodes failed: %s", e.err)
}

func retryable(err error) bool {
	return httputil.IsForbidden(err) || httputil.IsNetworkError(err)
}

type client struct {
	config    Config
	namenodes []string
	username  string
}

// NewClient creates a new Client.
func NewClient(config Config, namenodes []string, username string) (Client, error) {
	config.applyDefaults()
	if len(namenodes) == 0 {
		return nil, errors.New("namenodes required")
	}
	return &client{config, namenodes, username}, nil
}

// nameNodeBackOff returns the backoff used on all http requests to namenodes.
// We use a fairly aggressive backoff to handle failover.
//
// TODO(codyg): Normally it is be the responsibility of the farthest downstream
// client to handle retry, however the Mesos fetcher does not currently support
// retry, and thus namenode failovers have caused task launch failures. Ideally
// we can remove this once the Mesos fetcher is more reliable.
func (c *client) nameNodeBackOff() backoff.BackOff {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     2 * time.Second,
		RandomizationFactor: 0.05,
		Multiplier:          2,
		MaxInterval:         30 * time.Second,
		Clock:               backoff.SystemClock,
	}
	return backoff.WithMaxRetries(b, 5)
}

type exceededCapError error

// capBuffer is a buffer that returns errors if the buffer exceeds cap.
type capBuffer struct {
	cap int64
	buf *bytes.Buffer
}

func (b *capBuffer) Write(p []byte) (n int, err error) {
	if int64(len(p)+b.buf.Len()) > b.cap {
		return 0, exceededCapError(
			fmt.Errorf("buffer exceeded max capacity %s", memsize.Format(uint64(b.cap))))
	}
	return b.buf.Write(p)
}

type drainSrcError struct {
	err error
}

func (e drainSrcError) Error() string { return fmt.Sprintf("drain src: %s", e.err) }

func (c *client) Create(path string, src io.Reader) error {
	// We must be able to replay src in the event that uploading to the data node
	// fails halfway through the upload, thus we attempt to upcast src to an io.Seeker
	// for this purpose. If src is not an io.Seeker, we drain it to an in-memory buffer
	// that can be replayed.
	readSeeker, ok := src.(io.ReadSeeker)
	if !ok {
		var b []byte
		if buf, ok := src.(*bytes.Buffer); ok {
			// Optimization to avoid draining an existing buffer.
			b = buf.Bytes()
		} else {
			cbuf := &capBuffer{int64(c.config.BufferGuard), new(bytes.Buffer)}
			if _, err := io.Copy(cbuf, src); err != nil {
				return drainSrcError{err}
			}
			b = cbuf.buf.Bytes()
		}
		readSeeker = bytes.NewReader(b)
	}

	v := c.values()
	v.Set("op", "CREATE")
	v.Set("buffersize", strconv.FormatInt(int64(c.config.BufferSize), 10))
	v.Set("overwrite", "true")

	var nameresp, dataresp *http.Response
	var nnErr error
	for _, nn := range c.namenodes {
		nameresp, nnErr = httputil.Put(
			getURL(nn, path, v),
			httputil.SendRetry(httputil.RetryBackoff(c.nameNodeBackOff())),
			httputil.SendRedirect(func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			}),
			httputil.SendAcceptedCodes(http.StatusTemporaryRedirect, http.StatusPermanentRedirect))
		if nnErr != nil {
			if retryable(nnErr) {
				continue
			}
			return nnErr
		}
		defer nameresp.Body.Close()

		// Follow redirect location manually per WebHDFS protocol.
		loc, ok := nameresp.Header["Location"]
		if !ok || len(loc) == 0 {
			return fmt.Errorf("missing location field in response header: %s", nameresp.Header)
		}

		dataresp, nnErr = httputil.Put(
			loc[0],
			httputil.SendBody(readSeeker),
			httputil.SendAcceptedCodes(http.StatusCreated))
		if nnErr != nil {
			if retryable(nnErr) {
				// Reset reader for next retry.
				if _, err := readSeeker.Seek(0, io.SeekStart); err != nil {
					return fmt.Errorf("seek: %s", err)
				}
				continue
			}
			return nnErr
		}
		defer dataresp.Body.Close()

		return nil
	}
	return allNameNodesFailedError{nnErr}
}

func (c *client) Rename(from, to string) error {
	v := c.values()
	v.Set("op", "RENAME")
	v.Set("destination", to)

	var resp *http.Response
	var nnErr error
	for _, nn := range c.namenodes {
		resp, nnErr = httputil.Put(
			getURL(nn, from, v),
			httputil.SendRetry(httputil.RetryBackoff(c.nameNodeBackOff())))
		if nnErr != nil {
			if retryable(nnErr) {
				continue
			}
			return nnErr
		}
		resp.Body.Close()
		return nil
	}
	return allNameNodesFailedError{nnErr}
}

func (c *client) Mkdirs(path string) error {
	v := c.values()
	v.Set("op", "MKDIRS")
	v.Set("permission", "777")

	var resp *http.Response
	var nnErr error
	for _, nn := range c.namenodes {
		resp, nnErr = httputil.Put(
			getURL(nn, path, v),
			httputil.SendRetry(httputil.RetryBackoff(c.nameNodeBackOff())))
		if nnErr != nil {
			if retryable(nnErr) {
				continue
			}
			return nnErr
		}
		resp.Body.Close()
		return nil
	}
	return allNameNodesFailedError{nnErr}
}

func (c *client) Open(path string, dst io.Writer) error {
	v := c.values()
	v.Set("op", "OPEN")
	v.Set("buffersize", strconv.FormatInt(int64(c.config.BufferSize), 10))

	var resp *http.Response
	var nnErr error
	for _, nn := range c.namenodes {
		// We retry 400s here because experience has shown the datanode this
		// request gets redirected to is sometimes invalid, and will return a 400
		// error. By retrying the request, we hope to eventually get redirected
		// to a valid datanode.
		resp, nnErr = httputil.Get(
			getURL(nn, path, v),
			httputil.SendRetry(
				httputil.RetryBackoff(c.nameNodeBackOff()),
				httputil.RetryCodes(http.StatusBadRequest)))
		if nnErr != nil {
			if retryable(nnErr) {
				continue
			}
			if httputil.IsNotFound(nnErr) {
				return backenderrors.ErrBlobNotFound
			}
			return nnErr
		}
		defer resp.Body.Close()
		if n, err := io.Copy(dst, resp.Body); err != nil {
			return fmt.Errorf("copy response: %s", err)
		} else if n != resp.ContentLength {
			return fmt.Errorf(
				"transferred bytes %d does not match content length %d", n, resp.ContentLength)
		}
		return nil
	}
	return allNameNodesFailedError{nnErr}
}

func (c *client) GetFileStatus(path string) (FileStatus, error) {
	v := c.values()
	v.Set("op", "GETFILESTATUS")

	var resp *http.Response
	var nnErr error
	for _, nn := range c.namenodes {
		resp, nnErr = httputil.Get(
			getURL(nn, path, v),
			httputil.SendRetry(httputil.RetryBackoff(c.nameNodeBackOff())))
		if nnErr != nil {
			if retryable(nnErr) {
				continue
			}
			if httputil.IsNotFound(nnErr) {
				return FileStatus{}, backenderrors.ErrBlobNotFound
			}
			return FileStatus{}, nnErr
		}
		defer resp.Body.Close()
		var fsr fileStatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&fsr); err != nil {
			return FileStatus{}, fmt.Errorf("decode body: %s", err)
		}
		return fsr.FileStatus, nil
	}
	return FileStatus{}, allNameNodesFailedError{nnErr}
}

func (c *client) ListFileStatus(path string) ([]FileStatus, error) {
	v := c.values()
	v.Set("op", "LISTSTATUS")

	var resp *http.Response
	var nnErr error
	for _, nn := range c.namenodes {
		resp, nnErr = httputil.Get(
			getURL(nn, path, v),
			httputil.SendRetry(httputil.RetryBackoff(c.nameNodeBackOff())))
		if nnErr != nil {
			if retryable(nnErr) {
				continue
			}
			return nil, nnErr
		}
		defer resp.Body.Close()
		var lsr listStatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&lsr); err != nil {
			return nil, fmt.Errorf("decode body: %s", err)
		}
		return lsr.FileStatuses.FileStatus, nil
	}
	return nil, allNameNodesFailedError{nnErr}
}

func (c *client) values() url.Values {
	v := url.Values{}
	if c.username != "" {
		v.Set("user.name", c.username)
	}
	return v
}

func getURL(namenode, p string, v url.Values) string {
	endpoint := path.Join("/webhdfs/v1", p)
	return fmt.Sprintf("http://%s%s?%s", namenode, endpoint, v.Encode())
}
