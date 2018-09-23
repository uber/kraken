package s3backend

import (
	"io"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3 defines the operations we use in the s3 api. Useful for mocking.
type S3 interface {
	HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)

	Download(
		w io.WriterAt,
		input *s3.GetObjectInput,
		options ...func(*s3manager.Downloader)) (n int64, err error)

	Upload(
		input *s3manager.UploadInput,
		options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

type join struct {
	s3iface.S3API
	*s3manager.Downloader
	*s3manager.Uploader
}

var _ S3 = (*join)(nil)
