package s3backend

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
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
	config    Config
	pather    namepath.Pather
	s3Session s3iface.S3API
}

// NewClient creates a new Client.
func NewClient(config Config, auth AuthConfig, namespace string) (*Client, error) {
	config = config.applyDefaults()
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
	if auth.AccessKeyID != "" && auth.AccessSecretKey != "" {
		// These should be provided by langley or usecret
		log.Info("Using static s3 credentials")
		creds = credentials.NewStaticCredentials(auth.AccessKeyID, auth.AccessSecretKey, auth.SessionToken)
	} else {
		// fallback to shared credentials
		// default file name at ./aws/credendtials, profile is a namespace name
		log.Info("Using shared s3 credentials")
		if _, err := os.Stat(".aws/credentials"); os.IsNotExist(err) {
			return nil, errors.New(".aws/credentials file does not exist")
		}
		creds = credentials.NewSharedCredentials("", namespace)
	}

	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion(config.Region).WithCredentials(creds))

	return &Client{config, pather, svc}, nil
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
	path, err := c.pather.Path(name)
	if err != nil {
		return fmt.Errorf("path: %s", err)
	}

	// The S3 download API uses io.WriterAt to perform concurrent chunked download.
	// We attempt to upcast dst to io.WriterAt for this purpose, else we download into
	// in-memory buffer and drain it into dst after the download is finished.
	writerAt, ok := dst.(io.WriterAt)
	if !ok {
		log.With("name", name).Info("Using in-memory buffer for S3 download")
		writerAt = &capBuffer{int64(c.config.BufferGuard), aws.NewWriteAtBuffer([]byte{})}
	}

	downloader := s3manager.NewDownloaderWithClient(c.s3Session, func(d *s3manager.Downloader) {
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
	path, err := c.pather.Path(name)
	if err != nil {
		return fmt.Errorf("path: %s", err)
	}

	uploader := s3manager.NewUploaderWithClient(c.s3Session, func(u *s3manager.Uploader) {
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
	return ok && awsErr.Code() == s3.ErrCodeNoSuchKey
}
