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
package s3backend

import (
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/backend/namepath"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/rwutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"gopkg.in/yaml.v2"
)

const _s3 = "s3"

func init() {
	backend.Register(_s3, &factory{})
}

type factory struct{}

func (f *factory) Create(
	confRaw interface{}, authConfRaw interface{}) (backend.Client, error) {

	confBytes, err := yaml.Marshal(confRaw)
	if err != nil {
		return nil, errors.New("marshal s3 config")
	}
	authConfBytes, err := yaml.Marshal(authConfRaw)
	if err != nil {
		return nil, errors.New("marshal s3 auth config")
	}

	var config Config
	if err := yaml.Unmarshal(confBytes, &config); err != nil {
		return nil, errors.New("unmarshal s3 config")
	}
	var userAuth UserAuthConfig
	if err := yaml.Unmarshal(authConfBytes, &userAuth); err != nil {
		return nil, errors.New("unmarshal s3 auth config")
	}

	return NewClient(config, userAuth)
}

// Client implements a backend.Client for S3.
type Client struct {
	config Config
	pather namepath.Pather
	s3     S3
}

// Option allows setting optional Client parameters.
type Option func(*Client)

// WithS3 configures a Client with a custom S3 implementation.
func WithS3(s3 S3) Option {
	return func(c *Client) { c.s3 = s3 }
}

// NewClient creates a new Client for S3.
func NewClient(
	config Config, userAuth UserAuthConfig, opts ...Option) (*Client, error) {

	config.applyDefaults()
	if config.Username == "" {
		return nil, errors.New("invalid config: username required")
	}
	if config.Region == "" {
		return nil, errors.New("invalid config: region required")
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
	creds := credentials.NewStaticCredentials(
		auth.S3.AccessKeyID, auth.S3.AccessSecretKey, auth.S3.SessionToken)

	awsConfig := aws.NewConfig().WithRegion(config.Region).WithCredentials(creds)

	if config.Endpoint != "" {
		awsConfig = awsConfig.WithEndpoint(config.Endpoint)
	}

	if config.DisableSSL {
		awsConfig = awsConfig.WithDisableSSL(config.DisableSSL)
	}

	if config.S3ForcePathStyle {
		awsConfig = awsConfig.WithS3ForcePathStyle(config.S3ForcePathStyle)
	}

	api := s3.New(session.New(), awsConfig)

	downloader := s3manager.NewDownloaderWithClient(api, func(d *s3manager.Downloader) {
		d.PartSize = config.DownloadPartSize
		d.Concurrency = config.DownloadConcurrency
	})

	uploader := s3manager.NewUploaderWithClient(api, func(u *s3manager.Uploader) {
		u.PartSize = config.UploadPartSize
		u.Concurrency = config.UploadConcurrency
	})

	client := &Client{config, pather, join{api, downloader, uploader}}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

// Stat returns blob info for name.
func (c *Client) Stat(namespace, name string) (*core.BlobInfo, error) {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return nil, fmt.Errorf("blob path: %s", err)
	}
	output, err := c.s3.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, backenderrors.ErrBlobNotFound
		}
		return nil, err
	}
	var size int64
	if output.ContentLength != nil {
		size = *output.ContentLength
	}
	return core.NewBlobInfo(size), nil
}

// Download downloads the content from a configured bucket and writes the
// data to dst.
func (c *Client) Download(namespace, name string, dst io.Writer) error {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return fmt.Errorf("blob path: %s", err)
	}

	// The S3 download API uses io.WriterAt to perform concurrent chunked download.
	// We attempt to upcast dst to io.WriterAt for this purpose, else we download into
	// in-memory buffer and drain it into dst after the download is finished.
	writerAt, ok := dst.(io.WriterAt)
	if !ok {
		writerAt = rwutil.NewCappedBuffer(int(c.config.BufferGuard))
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(path),
	}
	if _, err := c.s3.Download(writerAt, input); err != nil {
		if isNotFound(err) {
			return backenderrors.ErrBlobNotFound
		}
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
	input := &s3manager.UploadInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(path),
		Body:   src,
	}
	_, err = c.s3.Upload(input, func(u *s3manager.Uploader) {
		u.LeavePartsOnError = false // Delete the parts if the upload fails.
	})
	return err
}

func isNotFound(err error) bool {
	awsErr, ok := err.(awserr.Error)
	return ok && (awsErr.Code() == s3.ErrCodeNoSuchKey || awsErr.Code() == "NotFound")
}

// List lists names with start with prefix.
func (c *Client) List(prefix string, opts ...backend.ListOption) (*backend.ListResult, error) {
	// For whatever reason, the S3 list API does not accept an absolute path
	// for prefix. Thus, the root is stripped from the input and added manually
	// to each output key.
	options := backend.DefaultListOptions()
	for _, opt := range opts {
		opt(options)
	}

	// If paginiated is enabled use the maximum number of keys requests from thhe options,
	// otherwise fall back to the configuration's max keys
	maxKeys := int64(c.config.ListMaxKeys)
	var continuationToken *string
	if options.Paginated {
		maxKeys = int64(options.MaxKeys)
		// An empty continuationToken should be left as nil when sending paginated list
		// requests to s3
		if options.ContinuationToken != "" {
			continuationToken = aws.String(options.ContinuationToken)
		}
	}

	var names []string
	nextContinuationToken := ""
	err := c.s3.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket:            aws.String(c.config.Bucket),
		MaxKeys:           aws.Int64(maxKeys),
		Prefix:            aws.String(path.Join(c.pather.BasePath(), prefix)[1:]),
		ContinuationToken: continuationToken,
	}, func(page *s3.ListObjectsV2Output, last bool) bool {
		for _, object := range page.Contents {
			if object.Key == nil {
				log.With(
					"prefix", prefix,
					"object", object).Error("List encountered nil S3 object key")
				continue
			}
			name, err := c.pather.NameFromBlobPath(path.Join("/", *object.Key))
			if err != nil {
				log.With("key", *object.Key).Errorf("Error converting blob path into name: %s", err)
				continue
			}
			names = append(names, name)
		}


		if int64(len(names)) < maxKeys {
			// Continue iterating pages to get more keys
			return true
		}

		// Attempt to capture the continuation token before we stop iterating pages
		if page.IsTruncated != nil && *page.IsTruncated && page.NextContinuationToken != nil {
			nextContinuationToken = *page.NextContinuationToken
		}

		return false
	})

	if err != nil {
		return nil, err
	}

	return &backend.ListResult{
		Names:             names,
		ContinuationToken: nextContinuationToken,
	}, nil
}
