// Copyright (c) 2016-2020 Uber Technologies, Inc.
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
package shadowbackend

import (
	"bytes"
	mockbackend "github.com/uber/kraken/mocks/lib/backend"
	"io"
	"reflect"
	"strings"
	"testing"
	"errors"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/backend/hdfsbackend"
	"github.com/uber/kraken/lib/backend/s3backend"
	"github.com/uber/kraken/lib/backend/sqlbackend"
	"github.com/uber/kraken/lib/backend/testfs"
)

type clientMocks struct {
	mockActive *mockbackend.MockClient
	mockShadow *mockbackend.MockClient
}

func newClientMocks(t *testing.T) (*clientMocks, func()) {
	ctrl := gomock.NewController(t)
	return &clientMocks{
		mockActive: mockbackend.NewMockClient(ctrl),
		mockShadow: mockbackend.NewMockClient(ctrl),
	}, ctrl.Finish
}

func newClient(mocks *clientMocks) *Client {
	var a interface{}
	var s interface{}
	a = mocks.mockActive
	ab, ok := a.(backend.Client)
	if !ok {
		panic("oh noes")
	}
	s = mocks.mockShadow
	sb, ok := s.(backend.Client)
	if !ok {
		panic("oh noes")
	}
	return &Client{active: ab, shadow: sb}
}

func TestClientFactory(t *testing.T) {
	sqlCfg := sqlbackend.Config{Dialect: "sqlite3", ConnectionString: ":memory:"}
	testfsCfg := testfs.Config{Addr: "localhost:1234", NamePath: "docker_tag", Root: "tags"}
	var auth s3backend.AuthConfig
	auth.S3.AccessKeyID = "accesskey"
	auth.S3.AccessSecretKey = "secret"
	authCfg := s3backend.UserAuthConfig{"test-user": auth}

	config := Config{
		ActiveClientConfig: map[string]interface{}{"sql": sqlCfg},
		ShadowClientConfig: map[string]interface{}{"testfs": testfsCfg},
	}

	f := factory{}
	c, err := f.Create(config, authCfg)
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestGetBackendClient(t *testing.T) {
	testCases := map[string]struct {
		cfg         map[string]interface{}
		authCfg     interface{}
		clientType  reflect.Type
		expectedErr string
	}{
		"sqlBackendSuccess": {
			cfg: map[string]interface{}{
				"sql": sqlbackend.Config{
					Dialect:          "sqlite3",
					ConnectionString: ":memory:"},
			},
			authCfg:    sqlbackend.UserAuthConfig{},
			clientType: reflect.TypeOf(&sqlbackend.Client{}),
		},
		"sqlBackendFailure": {
			cfg: map[string]interface{}{
				"sql": sqlbackend.Config{
					ConnectionString: "constring"},
			},
			expectedErr: "error connecting to database: sql: unknown driver \"\" (forgotten import?)",
		},
		"testfsBackendSuccess": {
			cfg: map[string]interface{}{
				"testfs": testfs.Config{
					Addr:     "localhost:1234",
					NamePath: "docker_tag",
					Root:     "tags"},
			},
			clientType: reflect.TypeOf(&testfs.Client{}),
		},
		"testfsBackendFailure": {
			cfg: map[string]interface{}{
				"testfs": testfs.Config{},
			},
			expectedErr: "no addr configured",
		},
		"hdfsBackendSuccess": {
			cfg: map[string]interface{}{
				"hdfs": hdfsbackend.Config{
					NameNodes:     []string{"some-name-node"},
					RootDirectory: "/root",
					NamePath:      "identity",
				},
			},
			clientType: reflect.TypeOf(&hdfsbackend.Client{}),
		},
		"hdfsBackendFailure": {
			cfg: map[string]interface{}{
				"hdfs": hdfsbackend.Config{},
			},
			expectedErr: "namepath: invalid pather identifier: empty",
		},
		"s3BackendSuccess": {
			cfg: map[string]interface{}{
				"s3": s3backend.Config{
					Username:      "test-user",
					Region:        "test-region",
					Bucket:        "test-bucket",
					NamePath:      "identity",
					RootDirectory: "/root",
				},
			},
			authCfg: s3backend.UserAuthConfig{
				"test-user": s3backend.AuthConfig{},
			},
			clientType: reflect.TypeOf(&s3backend.Client{}),
		},
		"s3BackendFailure": {
			cfg: map[string]interface{}{
				"s3": s3backend.Config{},
			},
			authCfg:     s3backend.UserAuthConfig{},
			expectedErr: "invalid config: username required",
		},
		"unsupportedBackend": {
			cfg: map[string]interface{}{
				"i_am_a_banana": backend.Config{},
			},
			expectedErr: "unsupported backend type 'i_am_a_banana'",
		},
	}

	for testName, tt := range testCases {
		t.Run(testName, func(t *testing.T) {

			client, err := getBackendClient(tt.cfg, tt.authCfg)

			if tt.expectedErr != "" {
				assert.EqualError(t, err, tt.expectedErr)
				assert.Nil(t, client)
			}

			if tt.clientType != nil {
				assert.NoError(t, err)
				require.NotNil(t, client)
				assert.Equal(t, tt.clientType, reflect.TypeOf(client))
			}
		})
	}
}

func TestStat(t *testing.T) {
	testCases := map[string]struct {
		activeErr error
		shadowErr error
		wantErr   string
	}{
		"both succeed": {
			nil,
			nil,
			"",
		},
		"both fail": {
			errors.New("some active error"),
			errors.New("some shadow error"),
			"[Stat] error in both backends for a:1 in namespace ''. active: 'some active error', shadow: 'some shadow error'",
		},
		"active fail shadow succeed": {
			errors.New("some active error"),
			nil,
			"some active error",
		},
		"active succeed shadow fail": {
			nil,
			errors.New("some shadow error"),
			"some shadow error",
		},
		"active not found shadow error": {
			backenderrors.ErrBlobNotFound,
			errors.New("some shadow error"),
			"[Stat] error in both backends for a:1 in namespace ''. active: '" + backenderrors.ErrBlobNotFound.Error() + "', shadow: 'some shadow error'",
		},
		"active error shadow not found": {
			errors.New("some active error"),
			backenderrors.ErrBlobNotFound,
			"[Stat] error in both backends for a:1 in namespace ''. active: 'some active error', shadow: '" + backenderrors.ErrBlobNotFound.Error() + "'",
		},
		"both not found": {
			backenderrors.ErrBlobNotFound,
			backenderrors.ErrBlobNotFound,
			backenderrors.ErrBlobNotFound.Error(),
		},
	}

	for testName, tt := range testCases {
		t.Run(testName, func(t *testing.T) {
			mocks, done := newClientMocks(t)
			defer done()

			if tt.activeErr != nil {
				mocks.mockActive.EXPECT().
					Stat("", "a:1").
					Return(nil, tt.activeErr)
			} else {
				mocks.mockActive.EXPECT().
					Stat("", "a:1").
					Return(&core.BlobInfo{}, nil)
			}

			if tt.shadowErr != nil {
				mocks.mockShadow.EXPECT().
					Stat("", "a:1").
					Return(nil, tt.shadowErr)
			} else {
				mocks.mockShadow.EXPECT().
					Stat("", "a:1").
					Return(&core.BlobInfo{}, nil)
			}

			client := newClient(mocks)

			res, err := client.Stat("", "a:1")

			if tt.wantErr != "" {
				assert.Nil(t, res)
				assert.EqualError(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, res)
			}
		})
	}
}

func TestDownloadActiveSuccess(t *testing.T) {
	mocks, done := newClientMocks(t)
	defer done()

	imageID := core.DigestFixture().String()
	w := new(bytes.Buffer)
	mocks.mockActive.EXPECT().
		Download("", "a:1", w).
		DoAndReturn(func(_ string, _ string, dst io.Writer) error {
			_, err := dst.Write([]byte(imageID))
			if err != nil {
				return err
			}
			return nil
		})

	client := newClient(mocks)

	err := client.Download("", "a:1", w)
	assert.NoError(t, err)
	assert.Equal(t, imageID, w.String())
}

func TestDownloadActiveNotFound(t *testing.T) {
	mocks, done := newClientMocks(t)
	defer done()

	mocks.mockActive.EXPECT().
		Download("", "a:1", gomock.Any()).
		Return(backenderrors.ErrBlobNotFound)

	client := newClient(mocks)

	err := client.Download("", "a:1", new(bytes.Buffer))
	assert.EqualError(t, err, backenderrors.ErrBlobNotFound.Error())
}

func TestUploadSuccess(t *testing.T) {
	mocks, done := newClientMocks(t)
	defer done()

	imageID := core.DigestFixture().String()
	newRepoTag := "avengers:scarlet_witch"
	mocks.mockActive.EXPECT().
		Upload("", newRepoTag, gomock.Any()).
		Return(nil)
	mocks.mockShadow.EXPECT().
		Upload("", newRepoTag, gomock.Any()).
		Return(nil)

	client := newClient(mocks)

	err := client.Upload("", newRepoTag, strings.NewReader(imageID))
	assert.NoError(t, err)
}

func TestUploadActiveFailure(t *testing.T) {
	mocks, done := newClientMocks(t)
	defer done()

	imageID := core.DigestFixture().String()
	newRepoTag := "avengers:scarlet_witch"
	expectedErr := errors.New("expected error")
	mocks.mockActive.EXPECT().
		Upload("", newRepoTag, gomock.Any()).
		Return(expectedErr)

	client := newClient(mocks)

	err := client.Upload("", newRepoTag, strings.NewReader(imageID))
	assert.EqualError(t, err, expectedErr.Error())
}

func TestUploadShadowFailure(t *testing.T) {
	mocks, done := newClientMocks(t)
	defer done()

	imageID := core.DigestFixture().String()
	newRepoTag := "avengers:scarlet_witch"
	expectedErr := errors.New("expected error")
	mocks.mockActive.EXPECT().
		Upload("", newRepoTag, gomock.Any()).
		Return(nil)
	mocks.mockShadow.EXPECT().
		Upload("", newRepoTag, gomock.Any()).
		Return(expectedErr)

	client := newClient(mocks)

	err := client.Upload("", newRepoTag, strings.NewReader(imageID))
	assert.EqualError(t, err, expectedErr.Error())
}

func TestListActiveSuccess(t *testing.T) {
	mocks, done := newClientMocks(t)
	defer done()

	results := backend.ListResult{
		Names: []string{
			"a:1",
			"a:2",
			"b:1",
			"c:1",
		},
	}

	mocks.mockActive.EXPECT().
		List("prefix", gomock.Any()).
		Return(&results, nil)

	client := newClient(mocks)

	res, err := client.List("prefix")
	require.NoError(t, err)
	assert.Equal(t, results.Names, res.Names)
}

func TestListActiveFailure(t *testing.T) {
	mocks, done := newClientMocks(t)
	defer done()

	mocks.mockActive.EXPECT().
		List("prefix", gomock.Any()).
		Return(nil, errors.New("expected error"))

	client := newClient(mocks)

	res, err := client.List("prefix")
	assert.EqualError(t, err, "expected error")
	assert.Nil(t, res)
}
