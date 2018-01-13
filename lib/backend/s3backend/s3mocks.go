package s3backend

import (
	"bytes"
	"io/ioutil"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// Mock mocks s3 (facepalm)
type Mock struct {
	object           []byte
	putObjectRequest *http.Request
	s3iface.S3API
}

// NewS3Mock returns new S3Mock
func NewS3Mock(o []byte, or *http.Request) *Mock {
	return &Mock{object: o, putObjectRequest: or}
}

// PutObjectWithContext puts object with context mock
func (m *Mock) PutObjectWithContext(aws.Context, *s3.PutObjectInput, ...request.Option) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

// PutObjectRequest pus object request mock
func (m *Mock) PutObjectRequest(*s3.PutObjectInput) (*request.Request, *s3.PutObjectOutput) {
	return &request.Request{HTTPRequest: m.putObjectRequest}, &s3.PutObjectOutput{}
}

// PutObject puts object mock
func (m *Mock) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

// GetObject mock
func (m *Mock) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	length := int64(len(m.object))
	pn := int64(1)

	return &s3.GetObjectOutput{
		Body:          ioutil.NopCloser(bytes.NewBuffer(m.object)),
		ContentLength: &length,
		PartsCount:    &pn,
	}, nil
}

// GetObjectWithContext gets objec mock
func (m *Mock) GetObjectWithContext(aws.Context, *s3.GetObjectInput, ...request.Option) (*s3.GetObjectOutput, error) {
	length := int64(len(m.object))
	pn := int64(1)

	return &s3.GetObjectOutput{
		Body:          ioutil.NopCloser(bytes.NewBuffer(m.object)),
		ContentLength: &length,
		PartsCount:    &pn,
	}, nil
}

// CompleteMultipartUpload completes multipart upload mock
func (m *Mock) CompleteMultipartUpload(*s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

// CompleteMultipartUploadWithContext completes maltipart upload with context mock
func (m *Mock) CompleteMultipartUploadWithContext(aws.Context, *s3.CompleteMultipartUploadInput, ...request.Option) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

// CompleteMultipartUploadRequest creates multipart upload request mock
func (m *Mock) CompleteMultipartUploadRequest(*s3.CompleteMultipartUploadInput) (*request.Request, *s3.CompleteMultipartUploadOutput) {
	return &request.Request{}, &s3.CompleteMultipartUploadOutput{}
}

// CreateMultipartUpload creates multipart upload mock
func (m *Mock) CreateMultipartUpload(*s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{}, nil
}

// CreateMultipartUploadWithContext creates multipart upload with contex mock
func (m *Mock) CreateMultipartUploadWithContext(aws.Context, *s3.CreateMultipartUploadInput, ...request.Option) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{}, nil
}

// UploadPart uploads a part to s3 mock
func (m *Mock) UploadPart(*s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{}, nil
}

// UploadPartWithContext uploads a part to s3 with context mock
func (m *Mock) UploadPartWithContext(aws.Context, *s3.UploadPartInput, ...request.Option) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{}, nil
}
