package s3backend

import (
	"errors"
	"fmt"
	"io"
	"path"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/backend/namepath"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

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

// NewClient creates a new Client.
func NewClient(config Config, userAuth UserAuthConfig, opts ...Option) (*Client, error) {
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

	api := s3.New(session.New(), aws.NewConfig().WithRegion(config.Region).WithCredentials(creds))

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
func (c *Client) Stat(name string) (*core.BlobInfo, error) {
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
func (c *Client) Download(name string, dst io.Writer) error {
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
func (c *Client) Upload(name string, src io.Reader) error {
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
func (c *Client) List(prefix string) ([]string, error) {
	// For whatever reason, the S3 list API does not accept an absolute path
	// for prefix. Thus, the root is stripped from the input and added manually
	// to each output key.
	var names []string
	err := c.s3.ListObjectsPages(&s3.ListObjectsInput{
		Bucket:  aws.String(c.config.Bucket),
		MaxKeys: aws.Int64(int64(c.config.ListMaxKeys)),
		Prefix:  aws.String(path.Join(c.pather.BasePath(), prefix)[1:]),
	}, func(page *s3.ListObjectsOutput, last bool) bool {
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
		return true
	})
	if err != nil {
		return nil, err
	}
	return names, nil
}
