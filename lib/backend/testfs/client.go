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
package testfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/backend/namepath"
	"github.com/uber/kraken/utils/httputil"

	"gopkg.in/yaml.v2"
)

const _testfs = "testfs"

func init() {
	backend.Register(_testfs, &factory{})
}

type factory struct{}

func (f *factory) Create(
	confRaw interface{}, authConfRaw interface{}) (backend.Client, error) {

	confBytes, err := yaml.Marshal(confRaw)
	if err != nil {
		return nil, errors.New("marshal testfs config")
	}

	var config Config
	if err := yaml.Unmarshal(confBytes, &config); err != nil {
		return nil, errors.New("unmarshal testfs config")
	}

	return NewClient(config)
}

// Client wraps HTTP calls to Server.
type Client struct {
	config Config
	pather namepath.Pather
}

// NewClient returns a new Client.
func NewClient(config Config) (*Client, error) {
	if config.Addr == "" {
		return nil, errors.New("no addr configured")
	}
	pather, err := namepath.New(config.Root, config.NamePath)
	if err != nil {
		return nil, fmt.Errorf("namepath: %s", err)
	}
	return &Client{config, pather}, nil
}

// Addr returns the configured server address.
func (c *Client) Addr() string {
	return c.config.Addr
}

// Stat returns blob info for name.
func (c *Client) Stat(namespace, name string) (*core.BlobInfo, error) {
	p, err := c.pather.BlobPath(name)
	if err != nil {
		return nil, fmt.Errorf("pather: %s", err)
	}
	resp, err := httputil.Head(
		fmt.Sprintf("http://%s/files/%s", c.config.Addr, p))
	if err != nil {
		if httputil.IsNotFound(err) {
			return nil, backenderrors.ErrBlobNotFound
		}
		return nil, err
	}
	size, err := strconv.ParseInt(resp.Header.Get("Size"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse size: %s", err)
	}
	return core.NewBlobInfo(size), nil
}

// Upload uploads src to name.
func (c *Client) Upload(namespace, name string, src io.Reader) error {
	p, err := c.pather.BlobPath(name)
	if err != nil {
		return fmt.Errorf("pather: %s", err)
	}
	_, err = httputil.Post(
		fmt.Sprintf("http://%s/files/%s", c.config.Addr, p),
		httputil.SendBody(src))
	return err
}

// Download downloads name to dst.
func (c *Client) Download(namespace, name string, dst io.Writer) error {
	p, err := c.pather.BlobPath(name)
	if err != nil {
		return fmt.Errorf("pather: %s", err)
	}
	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/files/%s", c.config.Addr, p))
	if err != nil {
		if httputil.IsNotFound(err) {
			return backenderrors.ErrBlobNotFound
		}
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(dst, resp.Body); err != nil {
		return fmt.Errorf("copy: %s", err)
	}
	return nil
}

// List lists names starting with prefix.
func (c *Client) List(prefix string, opts ...backend.ListOption) (*backend.ListResult, error) {
	options := backend.DefaultListOptions()
	for _, opt := range opts {
		opt(options)
	}

	if options.Paginated {
		return nil, errors.New("pagination not supported")
	}

	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/list/%s", c.config.Addr, path.Join(c.pather.BasePath(), prefix)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var paths []string
	if err := json.NewDecoder(resp.Body).Decode(&paths); err != nil {
		return nil, fmt.Errorf("json: %s", err)
	}
	var names []string
	for _, p := range paths {
		name, err := c.pather.NameFromBlobPath(p)
		if err != nil {
			return nil, fmt.Errorf("invalid path %s: %s", p, err)
		}
		names = append(names, name)
	}
	return &backend.ListResult{
		Names: names,
	}, nil
}
