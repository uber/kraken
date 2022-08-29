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
package registrybackend

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/backend/registrybackend/security"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/httputil"
	yaml "gopkg.in/yaml.v2"
)

const _registrytag = "registry_tag"

func init() {
	backend.Register(_registrytag, &tagClientFactory{})
}

type tagClientFactory struct{}

func (f *tagClientFactory) Create(
	confRaw interface{}, authConfRaw interface{}) (backend.Client, error) {

	confBytes, err := yaml.Marshal(confRaw)
	if err != nil {
		return nil, errors.New("marshal hdfs config")
	}
	var config Config
	if err := yaml.Unmarshal(confBytes, &config); err != nil {
		return nil, errors.New("unmarshal hdfs config")
	}
	return NewTagClient(config)
}

const _tagquery = "http://%s/v2/%s/manifests/%s"

// TagClient stats and downloads tag from registry.
type TagClient struct {
	config        Config
	authenticator security.Authenticator
}

// NewTagClient creates a new TagClient.
func NewTagClient(config Config) (*TagClient, error) {
	config = config.applyDefaults()
	authenticator, err := security.NewAuthenticator(config.Address, config.Security)
	if err != nil {
		return nil, fmt.Errorf("cannot create tag client authenticator: %s", err)
	}
	return &TagClient{
		config:        config,
		authenticator: authenticator,
	}, nil
}

// Stat sends a HEAD request to registry for a tag and returns the manifest size.
func (c *TagClient) Stat(namespace, name string) (*core.BlobInfo, error) {
	tokens := strings.Split(name, ":")
	if len(tokens) != 2 {
		return nil, fmt.Errorf("invald name %s: must be repo:tag", name)
	}
	repo, tag := tokens[0], tokens[1]

	opts, err := c.authenticator.Authenticate(repo)
	if err != nil {
		return nil, fmt.Errorf("get security opt: %s", err)
	}

	URL := fmt.Sprintf(_tagquery, c.config.Address, repo, tag)
	resp, err := httputil.Head(
		URL,
		append(
			opts,
			httputil.SendHeaders(map[string]string{"Accept": dockerutil.GetSupportedManifestTypes()}),
			httputil.SendAcceptedCodes(http.StatusOK, http.StatusNotFound),
		)...,
	)
	if err != nil {
		return nil, fmt.Errorf("check blob exists: %s", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, backenderrors.ErrBlobNotFound
	}
	size, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse blob size: %s", err)
	}
	return core.NewBlobInfo(size), nil
}

// Download gets the digest for a tag from registry.
func (c *TagClient) Download(namespace, name string, dst io.Writer) error {
	tokens := strings.Split(name, ":")
	if len(tokens) != 2 {
		return fmt.Errorf("invald name %s: must be repo:tag", name)
	}
	repo, tag := tokens[0], tokens[1]

	opts, err := c.authenticator.Authenticate(repo)
	if err != nil {
		return fmt.Errorf("get security opt: %s", err)
	}

	URL := fmt.Sprintf(_tagquery, c.config.Address, repo, tag)
	resp, err := httputil.Get(
		URL,
		append(
			opts,
			httputil.SendHeaders(map[string]string{"Accept": dockerutil.GetSupportedManifestTypes()}),
			httputil.SendAcceptedCodes(http.StatusOK, http.StatusNotFound),
		)...,
	)
	if err != nil {
		return fmt.Errorf("check blob exists: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return backenderrors.ErrBlobNotFound
	}

	_, digest, err := dockerutil.ParseManifest(resp.Body)
	if err != nil {
		return fmt.Errorf("parse manifest v2: %s", err)
	}
	if _, err := io.Copy(dst, strings.NewReader(digest.String())); err != nil {
		return fmt.Errorf("copy: %s", err)
	}
	return nil
}

// Upload is not supported as users can push directly to registry.
func (c *TagClient) Upload(namespace, name string, src io.Reader) error {
	return errors.New("not supported")
}

// List is not supported as users can list directly from registry.
func (c *TagClient) List(prefix string, opts ...backend.ListOption) (*backend.ListResult, error) {
	return nil, errors.New("not supported")
}
