// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package gcsbackend

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/c2h5oh/datasize"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	mockgcsbackend "github.com/uber/kraken/mocks/lib/backend/gcsbackend"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/mockutil"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/rwutil"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

const (
	_testPath = "root/test"
	_testName = "test"
)

type clientMocks struct {
	config   Config
	userAuth UserAuthConfig
	gcs      *mockgcsbackend.MockGCS
}

func newClientMocks(t *testing.T) (*clientMocks, func()) {
	ctrl := gomock.NewController(t)

	var auth AuthConfig
	auth.GCS.AccessBlob = "access_blob"

	return &clientMocks{
		config: Config{
			Username:      "test-user",
			Location:      "test-location",
			Bucket:        "test-bucket",
			NamePath:      "identity",
			RootDirectory: "root",
			ListMaxKeys:   5,
		},
		userAuth: UserAuthConfig{"test-user": auth},
		gcs:      mockgcsbackend.NewMockGCS(ctrl),
	}, ctrl.Finish
}

func (m *clientMocks) new() *Client {
	c, err := NewClient(m.config, m.userAuth, tally.NoopScope, zap.NewNop().Sugar(), WithGCS(m.gcs))
	if err != nil {
		panic(err)
	}
	return c
}

func TestClientFactory(t *testing.T) {
	require := require.New(t)

	config := Config{
		Username:      "test-user",
		Location:      "test-region",
		Bucket:        "test-bucket",
		NamePath:      "identity",
		RootDirectory: "root",
	}
	var auth AuthConfig
	auth.GCS.AccessBlob = "access_blob"
	userAuth := UserAuthConfig{"test-user": auth}
	f := factory{}
	masterAuth := backend.AuthConfig{f.Name(): userAuth}

	// "access_blob" is not a valid credentials JSON blob, so the underlying
	// storage client cannot be constructed.
	_, err := f.Create(config, masterAuth, tally.NoopScope, zap.NewNop().Sugar())
	require.Error(err)
	require.Contains(err.Error(), "new client")
}

func TestClientFactoryName(t *testing.T) {
	require.Equal(t, "gcs", (&factory{}).Name())
}

func TestClientFactoryAuthNotConfigured(t *testing.T) {
	require := require.New(t)

	config := Config{
		Username:      "test-user",
		Bucket:        "test-bucket",
		NamePath:      "identity",
		RootDirectory: "root",
	}
	f := factory{}

	_, err := f.Create(config, backend.AuthConfig{}, tally.NoopScope, zap.NewNop().Sugar())
	require.Error(err)
	require.Contains(err.Error(), "auth not configured for username")
}

func TestClientFactoryInvalidConfig(t *testing.T) {
	require := require.New(t)

	f := factory{}
	// username is a string, so a list fails to unmarshal into Config.
	confRaw := map[string]interface{}{"username": []string{"a", "b"}}

	_, err := f.Create(confRaw, backend.AuthConfig{}, tally.NoopScope, zap.NewNop().Sugar())
	require.Error(err)
	require.Contains(err.Error(), "unmarshal gcs config")
}

func TestClientFactoryInvalidAuthConfig(t *testing.T) {
	require := require.New(t)

	f := factory{}
	config := Config{Username: "test-user", Bucket: "test-bucket", NamePath: "identity"}
	// UserAuthConfig is a map, so a plain string fails to unmarshal into it.
	masterAuth := backend.AuthConfig{f.Name(): "not-a-map"}

	_, err := f.Create(config, masterAuth, tally.NoopScope, zap.NewNop().Sugar())
	require.Error(err)
	require.Contains(err.Error(), "unmarshal gcs auth config")
}

func TestNewClientInvalidConfig(t *testing.T) {
	var auth AuthConfig
	auth.GCS.AccessBlob = "access_blob"
	userAuth := UserAuthConfig{"test-user": auth}

	tests := []struct {
		desc     string
		config   Config
		userAuth UserAuthConfig
		expected string
	}{
		{
			desc:     "missing username",
			config:   Config{Bucket: "test-bucket", NamePath: "identity"},
			userAuth: userAuth,
			expected: "invalid config: username required",
		}, {
			desc:     "missing bucket",
			config:   Config{Username: "test-user", NamePath: "identity"},
			userAuth: userAuth,
			expected: "invalid config: bucket required",
		}, {
			desc: "absolute root directory",
			config: Config{
				Username:      "test-user",
				Bucket:        "test-bucket",
				NamePath:      "identity",
				RootDirectory: "/root",
			},
			userAuth: userAuth,
			expected: "invalid config: root_directory must not start with '/'",
		}, {
			desc:     "unknown namepath",
			config:   Config{Username: "test-user", Bucket: "test-bucket", NamePath: "nonsense"},
			userAuth: userAuth,
			expected: "namepath",
		}, {
			desc:     "auth not configured",
			config:   Config{Username: "other-user", Bucket: "test-bucket", NamePath: "identity"},
			userAuth: userAuth,
			expected: "auth not configured for username",
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			_, err := NewClient(test.config, test.userAuth, tally.NoopScope, zap.NewNop().Sugar())
			require.Error(err)
			require.Contains(err.Error(), test.expected)
		})
	}
}

func TestClientStat(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	mocks.gcs.EXPECT().ObjectAttrs(_testPath).Return(&storage.ObjectAttrs{Size: 100}, nil)

	info, err := client.Stat(core.NamespaceFixture(), _testName)
	require.NoError(err)
	require.Equal(core.NewBlobInfo(100), info)
}

func TestClientStatNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	mocks.gcs.EXPECT().ObjectAttrs(_testPath).Return(nil, storage.ErrObjectNotExist)

	_, err := client.Stat(core.NamespaceFixture(), _testName)
	require.Equal(backenderrors.ErrBlobNotFound, err)
}

func TestClientStatError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	expectedErr := errors.New("some gcs error")
	mocks.gcs.EXPECT().ObjectAttrs(_testPath).Return(nil, expectedErr)

	_, err := client.Stat(core.NamespaceFixture(), _testName)
	require.Equal(expectedErr, err)
}

// TestClientDownload verifies that a dst which already implements io.WriterAt
// is passed straight through to GCS, without an intermediate buffer.
func TestClientDownload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	data := randutil.Text(32)

	f, err := os.CreateTemp("", "")
	require.NoError(err)
	t.Cleanup(func() { require.NoError(os.Remove(f.Name())) })

	var downloadedTo io.WriterAt
	mocks.gcs.EXPECT().Download(_testPath, gomock.Any()).DoAndReturn(
		func(_ string, w io.WriterAt) (int64, error) {
			downloadedTo = w
			n, err := w.WriteAt(data, 0)
			return int64(n), err
		})

	require.NoError(client.Download(core.NamespaceFixture(), _testName, f))
	require.Same(f, downloadedTo)

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(err)
	result, err := io.ReadAll(f)
	require.NoError(err)
	require.Equal(data, result)
}

// TestClientDownloadWithBuffer verifies that a dst which does not implement
// io.WriterAt is downloaded into a capped buffer and drained afterwards.
func TestClientDownloadWithBuffer(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	data := randutil.Text(32)

	mocks.gcs.EXPECT().Download(
		_testPath,
		mockutil.MatchWriterAt(data),
	).Return(int64(len(data)), nil)

	// A plain io.Writer requires a buffer to download.
	w := make(rwutil.PlainWriter, len(data))
	require.NoError(client.Download(core.NamespaceFixture(), _testName, w))
	require.Equal(data, []byte(w))
}

func TestClientDownloadBufferGuard(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	data := randutil.Text(32)
	mocks.config.BufferGuard = datasize.ByteSize(len(data) - 1)

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	mocks.gcs.EXPECT().Download(_testPath, gomock.Any()).DoAndReturn(
		func(_ string, w io.WriterAt) (int64, error) {
			return 0, mustWriteAtError(w, data)
		})

	w := make(rwutil.PlainWriter, len(data))
	err := client.Download(core.NamespaceFixture(), _testName, w)
	require.Error(err)
	require.Contains(err.Error(), "buffer exceed max capacity")
}

// mustWriteAtError returns the error of writing b at offset 0 of w.
func mustWriteAtError(w io.WriterAt, b []byte) error {
	_, err := w.WriteAt(b, 0)
	return err
}

func TestClientDownloadError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	mocks.gcs.EXPECT().Download(_testPath, gomock.Any()).Return(
		int64(0), backenderrors.ErrBlobNotFound)

	var b bytes.Buffer
	err := client.Download(core.NamespaceFixture(), _testName, &b)
	require.Equal(backenderrors.ErrBlobNotFound, err)
}

func TestClientUpload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	data := randutil.Text(32)

	mocks.gcs.EXPECT().Upload(
		_testPath,
		mockutil.MatchReader(data),
	).Return(int64(len(data)), nil)

	require.NoError(client.Upload(core.NamespaceFixture(), _testName, bytes.NewReader(data)))
}

func TestClientUploadError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	expectedErr := errors.New("some gcs error")
	mocks.gcs.EXPECT().Upload(_testPath, gomock.Any()).Return(int64(0), expectedErr)

	err := client.Upload(core.NamespaceFixture(), _testName, bytes.NewReader(randutil.Text(32)))
	require.Equal(expectedErr, err)
}

func Alphabets(t *testing.T, maxIterate int) *AlphaIterator {
	it := &AlphaIterator{assert: require.New(t), maxIterate: maxIterate}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(
		it.next,
		func() int { return len(it.elems) },
		func() interface{} { e := it.elems; it.elems = nil; return e })
	return it
}

// Iterates from 0-maxIterate
type AlphaIterator struct {
	assert     *require.Assertions
	pageInfo   *iterator.PageInfo
	nextFunc   func() error
	elems      []string
	maxIterate int
}

func (it *AlphaIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

func (it *AlphaIterator) next(pageSize int, pageToken string) (string, error) {
	i := 0
	if pageToken != "" {
		var err error
		i, err = strconv.Atoi(pageToken)
		it.assert.NoError(err)
	}
	endCount := i + pageSize
	for ; i < endCount && i < it.maxIterate; i++ {
		it.elems = append(it.elems, "test/"+strconv.Itoa(i))
	}
	if i == it.maxIterate {
		return "", nil
	}
	return strconv.Itoa(i), nil
}

func TestClientList(t *testing.T) {
	require := require.New(t)
	maxIterate := 100

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	contToken := ""
	mocks.gcs.EXPECT().GetObjectIterator(
		_testPath,
	).AnyTimes().Return(Alphabets(t, maxIterate))
	for i := 0; i < maxIterate; {
		count := (rand.Int() % 10) + 1
		var expected []string
		var ret []string
		for j := i; j < (i+count) && j < maxIterate; j++ {
			expected = append(expected, "test/"+strconv.Itoa(j))
			ret = append(ret, _testPath+"/"+strconv.Itoa(j))
		}

		continuationToken := ""
		if (i + count) < maxIterate {
			continuationToken = strconv.Itoa(i + count)
		}
		mocks.gcs.EXPECT().NextPage(
			gomock.Any(),
		).Return(ret, continuationToken, nil)

		result, err := client.List("test", backend.ListWithPagination(),
			backend.ListWithMaxKeys(count),
			backend.ListWithContinuationToken(contToken))
		require.NoError(err)
		require.Equal(expected, result.Names)
		contToken = result.ContinuationToken
		i += count
	}
	require.Equal(contToken, "")
}

func TestClientListNotPaginated(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	mocks.gcs.EXPECT().GetObjectIterator(_testPath).Return(Alphabets(t, 2))
	mocks.gcs.EXPECT().NextPage(gomock.Any()).Return(
		[]string{_testPath + "/0", _testPath + "/1"}, "next-token", nil)

	result, err := client.List("test")
	require.NoError(err)
	require.Equal([]string{"test/0", "test/1"}, result.Names)
	// Continuation tokens are only returned for paginated listings.
	require.Equal("", result.ContinuationToken)
}

func TestClientListError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	mocks.gcs.EXPECT().Close().Return(nil)

	client := mocks.new()
	defer closers.Close(client)

	expectedErr := errors.New("some gcs error")
	mocks.gcs.EXPECT().GetObjectIterator(_testPath).Return(Alphabets(t, 1))
	mocks.gcs.EXPECT().NextPage(gomock.Any()).Return(nil, "", expectedErr)

	_, err := client.List("test")
	require.Equal(expectedErr, err)
}

func TestClientClose(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	mocks.gcs.EXPECT().Close().Return(nil)
	require.NoError(client.Close())
}

func TestClientCloseError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	expectedErr := errors.New("some gcs error")
	mocks.gcs.EXPECT().Close().Return(expectedErr)
	require.Equal(expectedErr, client.Close())
}

func TestClientCloseNilGCS(t *testing.T) {
	require.NoError(t, (&Client{}).Close())
}
