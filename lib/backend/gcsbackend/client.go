// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/transfermanager"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/backend/namepath"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/rwutil"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v2"
)

const _gcs = "gcs"

func init() {
	backend.Register(_gcs, &factory{})
}

type factory struct{}

// Name returns name of factory.
func (f *factory) Name() string {
	return _gcs
}

// Create returns a new gcsbackend client.
func (f *factory) Create(
	confRaw interface{}, authConfRaw backend.AuthConfig, stats tally.Scope, logger *zap.SugaredLogger) (backend.Client, error) {

	confBytes, err := yaml.Marshal(confRaw)
	if err != nil {
		return nil, fmt.Errorf("marshal gcs config: %w", err)
	}
	authConfBytes, err := yaml.Marshal(authConfRaw[f.Name()])
	if err != nil {
		return nil, fmt.Errorf("marshal gcs auth config: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(confBytes, &config); err != nil {
		return nil, fmt.Errorf("unmarshal gcs config: %w", err)
	}
	var userAuth UserAuthConfig
	if err := yaml.Unmarshal(authConfBytes, &userAuth); err != nil {
		return nil, fmt.Errorf("unmarshal gcs auth config: %w", err)
	}

	return NewClient(config, userAuth, stats, logger)
}

var _ backend.Client = &Client{}

// Client implements a GCS client.
type Client struct {
	config Config
	pather namepath.Pather
	gcs    GCS
	stats  tally.Scope
	logger *zap.SugaredLogger
}

// Option allows setting optional Client parameters.
type Option func(*Client)

// WithGCS configures a Client with a custom GCS implementation.
func WithGCS(gcs GCS) Option {
	return func(c *Client) { c.gcs = gcs }
}

// NewClient creates a new Client for GCS.
func NewClient(
	config Config, userAuth UserAuthConfig, stats tally.Scope, logger *zap.SugaredLogger, opts ...Option) (*Client, error) {

	config.applyDefaults()
	if config.Username == "" {
		return nil, errors.New("invalid config: username required")
	}
	if config.Bucket == "" {
		return nil, errors.New("invalid config: bucket required")
	}
	if path.IsAbs(config.RootDirectory) {
		return nil, errors.New("invalid config: root_directory must not start with '/'")
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
		client := &Client{
			gcs:    nil,
			config: config,
			pather: pather,
			stats:  stats,
			logger: logger,
		}
		for _, opt := range opts {
			opt(client)
		}
		return client, nil
	}

	ctx := context.Background()
	newClientOpts := []option.ClientOption{option.WithCredentialsJSON([]byte(auth.GCS.AccessBlob))}
	if config.PrivateServiceConnect != "" {
		newClientOpts = append(newClientOpts, option.WithEndpoint(config.PrivateServiceConnect))
	}

	sClient, err := storage.NewClient(ctx, newClientOpts...)
	if err != nil {
		return nil, fmt.Errorf("new client: %s", err)
	}

	gcsImpl, err := NewGCS(ctx, sClient, &config)
	if err != nil {
		return nil, fmt.Errorf("new gcs: %w", err)
	}

	client := &Client{
		gcs:    gcsImpl,
		config: config,
		pather: pather,
		stats:  stats,
		logger: logger,
	}

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

	writerAt, ok := dst.(io.WriterAt)
	if !ok {
		// TODO - consider returning an error here, instead of silently downloading into memory.
		writerAt = rwutil.NewCappedBuffer(int(c.config.BufferGuard))
	}

	_, err = c.gcs.Download(path, writerAt)
	if err != nil {
		return err
	}

	if capBuf, ok := writerAt.(*rwutil.CappedBuffer); ok {
		if err = capBuf.DrainInto(dst); err != nil {
			return err
		}
	}
	return nil
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

func (c *Client) Close() error {
	if c.gcs == nil {
		return nil
	}
	return c.gcs.Close()
}

// isObjectNotFound is helper function for identify non-existing object error.
func isObjectNotFound(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, storage.ErrBucketNotExist)
}

// GCSImpl implements GCS interaface.
type GCSImpl struct {
	ctx           context.Context
	storageClient *storage.Client
	config        *Config
	downloader    *transfermanager.Downloader
}

// NewGCS returns a new GCSImpl.
func NewGCS(ctx context.Context, storageClient *storage.Client, config *Config) (*GCSImpl, error) {
	downloader, err := transfermanager.NewDownloader(
		storageClient,
		transfermanager.WithWorkers(config.DownloadConcurrency),
		transfermanager.WithPartSize(int64(config.DownloadPartSize)),
		transfermanager.WithCallbacks(),
	)
	if err != nil {
		return nil, fmt.Errorf("new transfermanager downloader: %w", err)
	}
	return &GCSImpl{ctx, storageClient, config, downloader}, nil
}

// ObjectAttrs implements interface GCS.
func (g *GCSImpl) ObjectAttrs(objectName string) (*storage.ObjectAttrs, error) {
	handle := g.storageClient.Bucket(g.config.Bucket).Object(objectName)
	return handle.Attrs(g.ctx)
}

// Download implements interface GCS.
func (g *GCSImpl) Download(objectName string, w io.WriterAt) (int64, error) {
	ctx, cancel := context.WithTimeout(g.ctx, time.Duration(g.config.DownloadTimeoutSeconds)*time.Second)
	defer cancel()

	downloadOutput := make(chan *transfermanager.DownloadOutput)
	defer close(downloadOutput)
	in := &transfermanager.DownloadObjectInput{
		Bucket:      g.config.Bucket,
		Object:      objectName,
		Destination: w,
		Callback: func(o *transfermanager.DownloadOutput) {
			downloadOutput <- o
		},
	}

	err := g.downloader.DownloadObject(ctx, in)
	if err != nil {
		return 0, fmt.Errorf("download object: %w", err)
	}

	result := <-downloadOutput
	if result == nil {
		return 0, fmt.Errorf("unexpected error happened for path %q", objectName)
	}
	err = result.Err
	if err != nil {
		if isObjectNotFound(err) {
			return 0, backenderrors.ErrBlobNotFound
		}
		return 0, fmt.Errorf("wait downloads: %w", err)
	}

	return result.Attrs.Size, nil
}

// Upload implements interface GCS.
func (g *GCSImpl) Upload(objectName string, r io.Reader) (int64, error) {
	wc := g.storageClient.Bucket(g.config.Bucket).Object(objectName).NewWriter(g.ctx)
	wc.ContentType = "binary/octet-stream"
	wc.ChunkSize = int(g.config.UploadChunkSize)

	w, err := io.Copy(wc, r)
	if err != nil && err != io.EOF {
		return 0, err
	}

	if err := wc.Close(); err != nil {
		return 0, err
	}

	return w, nil
}

// GetObjectIterator implements interface GCS.
func (g *GCSImpl) GetObjectIterator(prefix string) iterator.Pageable {
	var query storage.Query

	query.Prefix = prefix
	return g.storageClient.Bucket(g.config.Bucket).Objects(g.ctx, &query)
}

// NextPage implements interface GCS.
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

func (g *GCSImpl) Close() error {
	totalErrors := make([]error, 0)
	if g.downloader != nil {
		_, err := g.downloader.WaitAndClose()
		if err != nil {
			totalErrors = append(totalErrors, err)
		}
	}
	if g.storageClient != nil {
		totalErrors = append(totalErrors, g.storageClient.Close())
	}
	if len(totalErrors) > 0 {
		return errors.Join(totalErrors...)
	}
	return nil
}
