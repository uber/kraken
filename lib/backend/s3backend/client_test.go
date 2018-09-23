package s3backend

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/lib/backend/s3backend"
	"code.uber.internal/infra/kraken/utils/mockutil"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/rwutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type clientMocks struct {
	config   Config
	userAuth UserAuthConfig
	s3       *mocks3backend.MockS3
}

func newClientMocks(t *testing.T) (*clientMocks, func()) {
	ctrl := gomock.NewController(t)

	var auth AuthConfig
	auth.S3.AccessKeyID = "accesskey"
	auth.S3.AccessSecretKey = "secret"

	return &clientMocks{
		config: Config{
			Username:      "test-user",
			Region:        "test-region",
			Bucket:        "test-bucket",
			NamePath:      "identity",
			RootDirectory: "/root",
		},
		userAuth: UserAuthConfig{"test-user": auth},
		s3:       mocks3backend.NewMockS3(ctrl),
	}, ctrl.Finish
}

func (m *clientMocks) new() *Client {
	c, err := NewClient(m.config, m.userAuth, WithS3(m.s3))
	if err != nil {
		panic(err)
	}
	return c
}

func TestClientStat(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	var length int64 = 100

	mocks.s3.EXPECT().HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("/root/test"),
	}).Return(&s3.HeadObjectOutput{ContentLength: &length}, nil)

	info, err := client.Stat("test")
	require.NoError(err)
	require.Equal(core.NewBlobInfo(100), info)
}

func TestClientDownload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	data := randutil.Text(32)

	mocks.s3.EXPECT().Download(
		mockutil.MatchWriterAt(data),
		&s3.GetObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String("/root/test"),
		},
	).Return(int64(len(data)), nil)

	var b bytes.Buffer
	require.NoError(client.Download("test", &b))
	require.Equal(data, b.Bytes())
}

func TestClientDownloadWithBuffer(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	data := randutil.Text(32)

	mocks.s3.EXPECT().Download(
		mockutil.MatchWriterAt(data),
		&s3.GetObjectInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String("/root/test"),
		},
	).Return(int64(len(data)), nil)

	// A plain io.Writer will require a buffer to download.
	w := make(rwutil.PlainWriter, len(data))
	require.NoError(client.Download("test", w))
	require.Equal(data, []byte(w))
}

func TestClientUpload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	data := bytes.NewReader(randutil.Text(32))

	mocks.s3.EXPECT().Upload(
		&s3manager.UploadInput{
			Bucket: aws.String("test-bucket"),
			Key:    aws.String("/root/test"),
			Body:   data,
		},
		gomock.Any(),
	).Return(nil, nil)

	require.NoError(client.Upload("test", data))
}
