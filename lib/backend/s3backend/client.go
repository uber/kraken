package s3backend

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/backend/blobinfo"
	"code.uber.internal/infra/kraken/lib/backend/namepath"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Client implements a backend.Client for S3.
type Client struct {
	config Config
	pather namepath.Pather
	svc    s3iface.S3API
}

// NewClient creates a new Client.
func NewClient(config Config, userAuth UserAuthConfig) (*Client, error) {
	config = config.applyDefaults()
	if config.Username == "" {
		return nil, errors.New("invalid config: username required")
	}
	if config.Region == "" {
		return nil, errors.New("invalid config: region required")
	}
	if config.Bucket == "" {
		return nil, errors.New("invalid config: bucket required")
	}

	pather, err := namepath.New(config.RootDirectory, config.NamePath)
	if err != nil {
		return nil, fmt.Errorf("namepath: %s", err)
	}

	var creds *credentials.Credentials
	if auth, ok := userAuth[config.Username]; ok {
		log.Info("S3 backend using Langley credentials")
		creds = credentials.NewStaticCredentials(
			auth.S3.AccessKeyID, auth.S3.AccessSecretKey, auth.S3.SessionToken)
	} else {
		log.Info("S3 backend using .aws/credentials")
		if _, err := os.Stat(".aws/credentials"); os.IsNotExist(err) {
			return nil, errors.New(".aws/credentials file does not exist")
		}
		creds = credentials.NewSharedCredentials("", config.Username)
	}

	svc := s3.New(session.New(), aws.NewConfig().WithRegion(config.Region).WithCredentials(creds))

	return &Client{config, pather, svc}, nil
}

// Stat returns blob info for name.
func (c *Client) Stat(name string) (*blobinfo.Info, error) {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return nil, fmt.Errorf("blob path: %s", err)
	}
	output, err := c.svc.HeadObject(&s3.HeadObjectInput{
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
	return blobinfo.New(size), nil
}

type exceededCapError error

// capBuffer is a buffer that returns errors if the buffer exceeds cap.
type capBuffer struct {
	cap int64
	buf *aws.WriteAtBuffer
}

func (b *capBuffer) WriteAt(p []byte, pos int64) (n int, err error) {
	if pos+int64(len(p)) > b.cap {
		return 0, exceededCapError(
			fmt.Errorf("buffer exceed max capacity %s", memsize.Format(uint64(b.cap))))
	}
	return b.buf.WriteAt(p, pos)
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
		log.With("name", name).Info("Using in-memory buffer for S3 download")
		writerAt = &capBuffer{int64(c.config.BufferGuard), aws.NewWriteAtBuffer([]byte{})}
	}

	downloader := s3manager.NewDownloaderWithClient(c.svc, func(d *s3manager.Downloader) {
		d.PartSize = c.config.DownloadPartSize // per part
		d.Concurrency = c.config.DownloadConcurrency
	})

	dlParams := &s3.GetObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(path),
	}

	log.Infof("Starting S3 download from remote backend: (bucket: %s, key %s)",
		c.config.Bucket, path)

	if _, err := downloader.Download(writerAt, dlParams); err != nil {
		if isNotFound(err) {
			return backenderrors.ErrBlobNotFound
		}
		return err
	}

	if cbuf, ok := writerAt.(*capBuffer); ok {
		if _, err := io.Copy(dst, bytes.NewReader(cbuf.buf.Bytes())); err != nil {
			return fmt.Errorf("drain buffer: %s", err)
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

	uploader := s3manager.NewUploaderWithClient(c.svc, func(u *s3manager.Uploader) {
		u.PartSize = c.config.UploadPartSize // per part,
		u.Concurrency = c.config.UploadConcurrency
	})

	upParams := &s3manager.UploadInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(path),
		Body:   src,
	}

	log.Infof("Starting S3 upload to remote backend: (bucket: %s, key: %s)",
		c.config.Bucket, path)

	// TODO(igor): support resumable uploads, for now we're ignoring UploadOutput
	// entirely
	_, err = uploader.Upload(upParams, func(u *s3manager.Uploader) {
		u.LeavePartsOnError = false // delete the parts if the upload fails.
	})
	return err
}

func isNotFound(err error) bool {
	awsErr, ok := err.(awserr.Error)
	return ok && awsErr.Code() == s3.ErrCodeNoSuchKey || awsErr.Code() == "NotFound"
}

// List TODO(codyg): Implement S3 list.
func (c *Client) List(dir string) ([]string, error) {
	return nil, errors.New("unimplemented")
}
