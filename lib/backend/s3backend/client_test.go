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
	"bytes"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/mocks/lib/backend/s3backend"
	"github.com/uber/kraken/utils/mockutil"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/rwutil"

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

func TestClientFactory(t *testing.T) {
	require := require.New(t)

	config := Config{
		Username:      "test-user",
		Region:        "test-region",
		Bucket:        "test-bucket",
		NamePath:      "identity",
		RootDirectory: "/root",
	}
	var auth AuthConfig
	auth.S3.AccessKeyID = "accesskey"
	auth.S3.AccessSecretKey = "secret"
	userAuth := UserAuthConfig{"test-user": auth}
	f := factory{}
	_, err := f.Create(config, userAuth)
	require.NoError(err)
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

	info, err := client.Stat(core.NamespaceFixture(), "test")
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
	require.NoError(client.Download(core.NamespaceFixture(), "test", &b))
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
	require.NoError(client.Download(core.NamespaceFixture(), "test", w))
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

	require.NoError(client.Upload(core.NamespaceFixture(), "test", data))
}

func TestClientList(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	mocks.s3.EXPECT().ListObjectsV2Pages(
		&s3.ListObjectsV2Input{
			Bucket:            aws.String("test-bucket"),
			MaxKeys:           aws.Int64(250),
			Prefix:            aws.String("root/test"),
		},
		gomock.Any(),
	).DoAndReturn(func(
		input *s3.ListObjectsV2Input,
		f func(page *s3.ListObjectsV2Output, last bool) bool) error {

		shouldContinue := f(&s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("root/test/a")},
				{Key: aws.String("root/test/b")},
			},
		}, false)

		if shouldContinue {
			f(&s3.ListObjectsV2Output{
				Contents: []*s3.Object{
					{Key: aws.String("root/test/c")},
					{Key: aws.String("root/test/d")},
				},
			}, true)
		}

		return nil
	})

	result, err := client.List("test")
	require.NoError(err)
	require.Equal([]string{"test/a", "test/b", "test/c", "test/d"}, result.Names)
}

func TestClientListPaginated(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	mocks.s3.EXPECT().ListObjectsV2Pages(
		&s3.ListObjectsV2Input{
			Bucket:            aws.String("test-bucket"),
			MaxKeys:           aws.Int64(2),
			Prefix:            aws.String("root/test"),
		},
		gomock.Any(),
	).DoAndReturn(func(
		input *s3.ListObjectsV2Input,
		f func(page *s3.ListObjectsV2Output, last bool) bool) error {

		f(&s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("root/test/a")},
				{Key: aws.String("root/test/b")},
			},
			IsTruncated:           aws.Bool(true),
			NextContinuationToken: aws.String("test-continuation-token"),
		}, false)

		return nil
	})

	mocks.s3.EXPECT().ListObjectsV2Pages(
		&s3.ListObjectsV2Input{
			Bucket:            aws.String("test-bucket"),
			MaxKeys:           aws.Int64(2),
			Prefix:            aws.String("root/test"),
			ContinuationToken: aws.String("test-continuation-token"),
		},
		gomock.Any(),
	).DoAndReturn(func(
		input *s3.ListObjectsV2Input,
		f func(page *s3.ListObjectsV2Output, last bool) bool) error {

		f(&s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("root/test/c")},
				{Key: aws.String("root/test/d")},
			},
			IsTruncated: aws.Bool(false),
		}, true)

		return nil
	})

	result, err := client.List("test",
		backend.ListWithPagination(),
		backend.ListWithMaxKeys(2),
	)
	require.NoError(err)
	require.Equal([]string{"test/a", "test/b"}, result.Names)
	require.Equal("test-continuation-token", result.ContinuationToken)

	result, err = client.List("test",
		backend.ListWithPagination(),
		backend.ListWithMaxKeys(2),
		backend.ListWithContinuationToken(result.ContinuationToken),
	)
	require.NoError(err)
	require.Equal([]string{"test/c", "test/d"}, result.Names)
	require.Equal("", result.ContinuationToken)
}
