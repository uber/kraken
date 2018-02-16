package s3backend

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Client implements downloading/uploading object from/to S3
type client struct {
	config    Config
	s3Session s3iface.S3API // S3 session
}

// newClient creates s3 client from input parameters
func newClient(config Config) (*client, error) {
	config = config.applyDefaults()
	if config.Region == "" {
		return nil, errors.New("invalid config: region required")
	}
	if config.Bucket == "" {
		return nil, errors.New("invalid config: bucket required")
	}

	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion(config.Region))

	return &client{s3Session: svc, config: config}, nil
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
func (c *client) download(name string, dst io.Writer) error {

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
		Key:    aws.String(name),
	}

	log.Infof("Starting S3 download from remote backend: (bucket: %s, key %s)",
		c.config.Bucket, name)

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
func (c *client) upload(name string, src io.Reader) error {
	uploader := s3manager.NewUploaderWithClient(c.s3Session, func(u *s3manager.Uploader) {
		u.PartSize = c.config.UploadPartSize // per part,
		u.Concurrency = c.config.UploadConcurrency
	})

	upParams := &s3manager.UploadInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(name),
		Body:   src,
	}

	log.Infof("Starting S3 upload to remote backend: (bucket: %s, key: %s)",
		c.config.Bucket, name)

	// TODO(igor): support resumable uploads, for now we're ignoring UploadOutput
	// entirely
	_, err := uploader.Upload(upParams, func(u *s3manager.Uploader) {
		u.LeavePartsOnError = false // delete the parts if the upload fails.
	})
	return err
}

func isNotFound(err error) bool {
	awsErr, ok := err.(awserr.Error)
	return ok && awsErr.Code() == s3.ErrCodeNoSuchKey
}
