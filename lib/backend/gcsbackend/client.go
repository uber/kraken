// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package gcsbackend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/backend/namepath"
	"github.com/uber/kraken/utils/log"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v2"
)

const _gcs = "gcs"

func init() {
	backend.Register(_gcs, &factory{})
}

type factory struct{}

func (f *factory) Create(
	confRaw interface{}, authConfRaw interface{}) (backend.Client, error) {

	confBytes, err := yaml.Marshal(confRaw)
	if err != nil {
		return nil, errors.New("marshal gcs config")
	}
	authConfBytes, err := yaml.Marshal(authConfRaw)
	if err != nil {
		return nil, errors.New("marshal gcs auth config")
	}

	var config Config
	if err := yaml.Unmarshal(confBytes, &config); err != nil {
		return nil, errors.New("unmarshal gcs config")
	}
	var userAuth UserAuthConfig
	if err := yaml.Unmarshal(authConfBytes, &userAuth); err != nil {
		return nil, errors.New("unmarshal gcs auth config")
	}

	return NewClient(config, userAuth)
}

// Client implements a backend.Client for GCS.
type Client struct {
	config Config
	pather namepath.Pather
	gcs    GCS
}

// Option allows setting optional Client parameters.
type Option func(*Client)

// WithGCS configures a Client with a custom GCS implementation.
func WithGCS(gcs GCS) Option {
	return func(c *Client) { c.gcs = gcs }
}

// NewClient creates a new Client for GCS.
func NewClient(
	config Config, userAuth UserAuthConfig, opts ...Option) (*Client, error) {

	config.applyDefaults()
	if config.Username == "" {
		return nil, errors.New("invalid config: username required")
	}
	if config.Bucket == "" {
		return nil, errors.New("invalid config: bucket required")
	}
	if !path.IsAbs(config.RootDirectory) {
		return nil, errors.New("invalid config: root_directory must be absolute path")
	}

	pather, err := namepath.New(config.RootDirectory, config.NamePath)
	if err != nil {
		return nil, fmt.Errorf("namepath: %s", err)
	}

	auth, ok := userAuth[config.Username]
	if !ok {
		return nil, errors.New("auth not configured for username")
	}

	if len(opts) > 0 {
		// For mock.
		client := &Client{config, pather, nil}
		for _, opt := range opts {
			opt(client)
		}
		return client, nil
	}

	ctx := context.Background()
	sClient, err := storage.NewClient(ctx,
		option.WithCredentialsJSON([]byte(auth.GCS.AccessBlob)))
	if err != nil {
		return nil, fmt.Errorf("invalid gcs credentials: %s", err)
	}

	client := &Client{config, pather,
		NewGCS(ctx, sClient.Bucket(config.Bucket), &config)}

	log.Infof("Initalized GCS backend with config: %s", config)
	return client, nil
}

// Stat returns blob info for name.
func (c *Client) Stat(namespace, name string) (*core.BlobInfo, error) {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return nil, fmt.Errorf("blob path: %s", err)
	}

	objectAttrs, err := c.gcs.ObjectAttrs(path)
	if err != nil {
		if isObjectNotFound(err) {
			return nil, backenderrors.ErrBlobNotFound
		}
		return nil, err
	}

	return core.NewBlobInfo(objectAttrs.Size), nil
}

// Download downloads the content from a configured bucket and writes the
// data to dst.
func (c *Client) Download(namespace, name string, dst io.Writer) error {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return fmt.Errorf("blob path: %s", err)
	}

	_, err = c.gcs.Download(path, dst)
	return err
}

// Upload uploads src to a configured bucket.
func (c *Client) Upload(namespace, name string, src io.Reader) error {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return fmt.Errorf("blob path: %s", err)
	}

	_, err = c.gcs.Upload(path, src)
	return err
}

// List lists names that start with prefix.
func (c *Client) List(prefix string, opts ...backend.ListOption) (*backend.ListResult, error) {
	options := backend.DefaultListOptions()
	for _, opt := range opts {
		opt(options)
	}

	absPrefix := path.Join(c.pather.BasePath(), prefix)
	pageIterator := c.gcs.GetObjectIterator(absPrefix)

	maxKeys := c.config.ListMaxKeys
	paginationToken := ""
	if options.Paginated {
		maxKeys = options.MaxKeys
		paginationToken = options.ContinuationToken
	}

	pager := iterator.NewPager(pageIterator, maxKeys, paginationToken)
	blobs, continuationToken, err := c.gcs.NextPage(pager)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, b := range blobs {
		name, err := c.pather.NameFromBlobPath(b)
		if err != nil {
			log.With("blob", b).Errorf("Error converting blob path into name: %s", err)
			continue
		}
		names = append(names, name)
	}
	result := &backend.ListResult{
		Names:             names,
		ContinuationToken: continuationToken,
	}

	if !options.Paginated {
		result.ContinuationToken = ""
	}
	return result, nil
}

// isObjectNotFound is helper function for identify non-existing object error.
func isObjectNotFound(err error) bool {
	return err == storage.ErrObjectNotExist || err == storage.ErrBucketNotExist
}

// GCSImpl implements GCS interaface.
type GCSImpl struct {
	ctx    context.Context
	bucket *storage.BucketHandle
	config *Config
}

func NewGCS(ctx context.Context, bucket *storage.BucketHandle,
	config *Config) *GCSImpl {

	return &GCSImpl{ctx, bucket, config}
}

func (g *GCSImpl) ObjectAttrs(objectName string) (*storage.ObjectAttrs, error) {
	handle := g.bucket.Object(objectName)
	return handle.Attrs(g.ctx)
}

func (g *GCSImpl) Download(objectName string, w io.Writer) (int64, error) {
	rc, err := g.bucket.Object(objectName).NewReader(g.ctx)
	if err != nil {
		if isObjectNotFound(err) {
			return 0, backenderrors.ErrBlobNotFound
		}
		return 0, err
	}
	defer rc.Close()

	r, err := io.CopyN(w, rc, int64(g.config.BufferGuard))
	if err != nil && err != io.EOF {
		return 0, err
	}

	return r, nil
}

func (g *GCSImpl) Upload(objectName string, r io.Reader) (int64, error) {
	wc := g.bucket.Object(objectName).NewWriter(g.ctx)
	wc.ChunkSize = int(g.config.UploadChunkSize)

	w, err := io.CopyN(wc, r, int64(g.config.UploadChunkSize))
	if err != nil && err != io.EOF {
		return 0, err
	}

	if err := wc.Close(); err != nil {
		return 0, err
	}

	return w, nil
}

func (g *GCSImpl) GetObjectIterator(prefix string) iterator.Pageable {
	var query storage.Query

	query.Prefix = prefix
	return g.bucket.Objects(g.ctx, &query)
}

func (g *GCSImpl) NextPage(pager *iterator.Pager) ([]string, string,
	error) {

	var objectAttrs []*storage.ObjectAttrs
	continuationToken, err := pager.NextPage(&objectAttrs)
	if err != nil {
		return nil, "", err
	}

	names := make([]string, len(objectAttrs))
	for idx, objectAttr := range objectAttrs {
		names[idx] = objectAttr.Name
	}
	return names, continuationToken, nil
}
