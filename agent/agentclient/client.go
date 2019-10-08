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
package agentclient

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/httputil"
)

// Client errors.
var (
	ErrTagNotFound = errors.New("tag not found")
)

// Client defines a client for accessing the agent server.
type Client interface {
	GetTag(tag string) (core.Digest, error)
	Download(namespace string, d core.Digest) (io.ReadCloser, error)
}

// HTTPClient provides a wrapper for HTTP operations on an agent.
type HTTPClient struct {
	addr string
}

// New creates a new client for an agent at addr.
func New(addr string) *HTTPClient {
	return &HTTPClient{addr}
}

// GetTag resolves tag into a digest. Returns ErrTagNotFound if the tag does
// not exist.
func (c *HTTPClient) GetTag(tag string) (core.Digest, error) {
	resp, err := httputil.Get(fmt.Sprintf("http://%s/tags/%s", c.addr, url.PathEscape(tag)))
	if err != nil {
		if httputil.IsNotFound(err) {
			return core.Digest{}, ErrTagNotFound
		}
		return core.Digest{}, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return core.Digest{}, fmt.Errorf("read body: %s", err)
	}
	d, err := core.ParseSHA256Digest(string(b))
	if err != nil {
		return core.Digest{}, fmt.Errorf("parse digest: %s", err)
	}
	return d, nil
}

// Download returns the blob of d. Callers should close the returned ReadCloser
// when done reading the blob.
func (c *HTTPClient) Download(namespace string, d core.Digest) (io.ReadCloser, error) {
	resp, err := httputil.Get(
		fmt.Sprintf(
			"http://%s/namespace/%s/blobs/%s",
			c.addr, url.PathEscape(namespace), d))
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}
