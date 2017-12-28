package s3

import (
	"code.uber.internal/infra/kraken/lib/fileio"

	"code.uber.internal/infra/kraken/utils/memsize"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Config defines s3 connection specific
// parameters and authetication credentials
type Config struct {
	Region string `yaml:"region"` // AWS S3 region
	Bucket string `yaml:"bucket"` // S3 bucket

	UploadPartSize   int64 `yaml:"upload_part_size"`   // part size s3 manager uses for upload
	DownloadPartSize int64 `yaml:"download_part_size"` // part size s3 manager uses for download

	UploadConcurrency   int `yaml:"upload_concurrency"`   // # of concurrent go-routines s3 manager uses for upload
	DownloadConcurrency int `yaml:"donwload_concurrency"` // # of concurrent go-routines s3 manager uses for download
}

// Client implements downloading/uploading object from/to S3
type Client struct {
	config    Config
	s3Session s3iface.S3API // S3 session
}

func (c Config) applyDefaults() Config {
	if c.UploadPartSize == 0 {
		c.UploadPartSize = int64(64 * memsize.MB)
	}
	if c.DownloadPartSize == 0 {
		c.DownloadPartSize = int64(64 * memsize.MB)
	}
	if c.UploadConcurrency == 0 {
		c.UploadConcurrency = 10
	}
	if c.DownloadConcurrency == 0 {
		c.DownloadConcurrency = 10
	}
	return c
}

// NewS3Client creates s3 client from input parameters
func NewS3Client(config Config) *Client {
	config = config.applyDefaults()

	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion(config.Region))

	return &Client{s3Session: svc, config: config}
}

// Download downloads the content from a given input bucket writing
// data into provided writer
func (s3f *Client) Download(w fileio.Writer, src string) (int64, error) {
	downloader := s3manager.NewDownloaderWithClient(s3f.s3Session, func(d *s3manager.Downloader) {
		d.PartSize = s3f.config.DownloadPartSize // per part
		d.Concurrency = s3f.config.DownloadConcurrency
	})

	dlParams := &s3.GetObjectInput{
		Bucket: aws.String(s3f.config.Bucket),
		Key:    aws.String(src),
	}

	n, err := downloader.Download(w, dlParams)
	return n, err
}

// Upload uploads the content for a given input bucket and key reading
// data from a provided reader
func (s3f *Client) Upload(r fileio.Reader, dst string) error {
	uploader := s3manager.NewUploaderWithClient(s3f.s3Session, func(u *s3manager.Uploader) {
		u.PartSize = s3f.config.UploadPartSize // per part,
		u.Concurrency = s3f.config.UploadConcurrency
	})

	upParams := &s3manager.UploadInput{
		Bucket: aws.String(s3f.config.Bucket),
		Key:    aws.String(dst),
		Body:   r,
	}

	// TODO(igor): support resumable uploads, for now we're ignoring UploadOutput
	// entirely
	_, err := uploader.Upload(upParams, func(u *s3manager.Uploader) {
		u.LeavePartsOnError = false // delete the parts if the upload fails.
	})
	return err
}
