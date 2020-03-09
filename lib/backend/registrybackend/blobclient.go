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

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/backend/registrybackend/security"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"
	yaml "gopkg.in/yaml.v2"
)

const _registryblob = "registry_blob"

func init() {
	backend.Register(_registryblob, &blobClientFactory{})
}

type blobClientFactory struct{}

func (f *blobClientFactory) Create(
	confRaw interface{}, authConfRaw interface{}) (backend.Client, error) {

	confBytes, err := yaml.Marshal(confRaw)
	if err != nil {
		return nil, errors.New("marshal hdfs config")
	}
	var config Config
	if err := yaml.Unmarshal(confBytes, &config); err != nil {
		return nil, errors.New("unmarshal hdfs config")
	}
	return NewBlobClient(config)
}

const _layerquery = "http://%s/v2/%s/blobs/sha256:%s"
const _manifestquery = "http://%s/v2/%s/manifests/sha256:%s"

// BlobClient stats and downloads blob from registry.
type BlobClient struct {
	config        Config
	authenticator security.Authenticator
}

// NewBlobClient creates a new BlobClient.
func NewBlobClient(config Config) (*BlobClient, error) {
	config = config.applyDefaults()
	authenticator, err := security.NewAuthenticator(config.Address, config.Security)
	if err != nil {
		return nil, fmt.Errorf("cannot create tag client authenticator: %s", err)
	}
	return &BlobClient{
		config:        config,
		authenticator: authenticator,
	}, nil
}

// Stat sends a HEAD request to registry for a blob and returns the blob size.
func (c *BlobClient) Stat(namespace, name string) (*core.BlobInfo, error) {
	opts, err := c.authenticator.Authenticate(namespace)
	if err != nil {
		return nil, fmt.Errorf("get security opt: %s", err)
	}

	info, err := c.statHelper(namespace, name, _layerquery, opts)
	if err != nil && err == backenderrors.ErrBlobNotFound {
		// Docker registry does not support querying manifests with blob path.
		log.Infof("Blob %s unknown to registry. Tring to stat manifest instead", name)
		info, err = c.statHelper(namespace, name, _manifestquery, opts)
	}
	return info, err
}

// Download gets a blob from registry.
func (c *BlobClient) Download(namespace, name string, dst io.Writer) error {
	opts, err := c.authenticator.Authenticate(namespace)
	if err != nil {
		return fmt.Errorf("get security opt: %s", err)
	}

	err = c.downloadHelper(namespace, name, _layerquery, dst, opts)
	if err != nil && err == backenderrors.ErrBlobNotFound {
		// Docker registry does not support querying manifests with blob path.
		log.Infof("Blob %s unknown to registry. Tring to download manifest instead", name)
		err = c.downloadHelper(namespace, name, _manifestquery, dst, opts)
	}
	return err
}

func (c *BlobClient) statHelper(namespace, name, query string, opts []httputil.SendOption) (*core.BlobInfo, error) {
	URL := fmt.Sprintf(query, c.config.Address, namespace, name)
	resp, err := httputil.Head(
		URL,
		append(opts, httputil.SendAcceptedCodes(http.StatusOK))...,
	)
	if err != nil {
		if httputil.IsNotFound(err) {
			return nil, backenderrors.ErrBlobNotFound
		}
		return nil, fmt.Errorf("head blob: %s", err)
	}

	size, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse blob size: %s", err)
	}
	return core.NewBlobInfo(size), nil
}

func (c *BlobClient) downloadHelper(namespace, name, query string, dst io.Writer, opts []httputil.SendOption) error {
	URL := fmt.Sprintf(query, c.config.Address, namespace, name)
	resp, err := httputil.Get(
		URL,
		append(
			opts,
			httputil.SendAcceptedCodes(http.StatusOK),
			httputil.SendTimeout(c.config.Timeout),
		)...,
	)
	if err != nil {
		if httputil.IsNotFound(err) {
			return backenderrors.ErrBlobNotFound
		}
		return fmt.Errorf("get blob: %s", err)
	}
	defer resp.Body.Close()

	if _, err := io.Copy(dst, resp.Body); err != nil {
		return fmt.Errorf("copy: %s", err)
	}
	return nil
}

// Upload is not supported as users can push directly to registry.
func (c *BlobClient) Upload(namespace, name string, src io.Reader) error {
	return errors.New("not supported")
}

// List is not supported for blobs.
func (c *BlobClient) List(prefix string, opts ...backend.ListOption) (*backend.ListResult, error) {
	return nil, errors.New("not supported")
}
