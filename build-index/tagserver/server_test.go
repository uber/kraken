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
package tagserver

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/build-index/tagstore"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/lib/persistedretry/tagreplication"
	"github.com/uber/kraken/mocks/build-index/tagclient"
	"github.com/uber/kraken/mocks/build-index/tagstore"
	"github.com/uber/kraken/mocks/build-index/tagtype"
	"github.com/uber/kraken/mocks/lib/backend"
	"github.com/uber/kraken/mocks/lib/persistedretry"
	"github.com/uber/kraken/mocks/origin/blobclient"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	_testNamespace = ".*"
	_testOrigin    = "some-dns-record"
	_testRemote    = "remote-build-index"
	_testNeighbor  = "local-build-index:3000"
)

type serverMocks struct {
	ctrl                  *gomock.Controller
	config                Config
	backends              *backend.Manager
	backendClient         *mockbackend.MockClient
	remotes               tagreplication.Remotes
	tagReplicationManager *mockpersistedretry.MockManager
	provider              *mocktagclient.MockProvider
	depResolver           *mocktagtype.MockDependencyResolver
	originClient          *mockblobclient.MockClusterClient
	store                 *mocktagstore.MockStore
	neighbors             hostlist.List
}

func newServerMocks(t *testing.T) (*serverMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	tagReplicationManager := mockpersistedretry.NewMockManager(ctrl)

	backends := backend.ManagerFixture()
	backendClient := mockbackend.NewMockClient(ctrl)
	require.NoError(t, backends.Register(_testNamespace, backendClient))

	remotes, err := tagreplication.RemotesConfig{
		_testRemote: []string{_testNamespace},
	}.Build()
	if err != nil {
		t.Fatal(err)
	}

	provider := mocktagclient.NewMockProvider(ctrl)

	originClient := mockblobclient.NewMockClusterClient(ctrl)

	depResolver := mocktagtype.NewMockDependencyResolver(ctrl)

	store := mocktagstore.NewMockStore(ctrl)

	return &serverMocks{
		ctrl:                  ctrl,
		config:                Config{DuplicateReplicateStagger: 20 * time.Minute},
		backends:              backends,
		backendClient:         backendClient,
		remotes:               remotes,
		tagReplicationManager: tagReplicationManager,
		provider:              provider,
		originClient:          originClient,
		depResolver:           depResolver,
		store:                 store,
		neighbors:             hostlist.Fixture(_testNeighbor),
	}, cleanup.Run
}

func (m *serverMocks) client() *mocktagclient.MockClient {
	return mocktagclient.NewMockClient(m.ctrl)
}

func (m *serverMocks) handler() http.Handler {
	return New(
		m.config,
		tally.NoopScope,
		m.backends,
		_testOrigin,
		m.originClient,
		m.neighbors,
		m.store,
		m.remotes,
		m.tagReplicationManager,
		m.provider,
		m.depResolver).Handler()
}

func newClusterClient(addr string) tagclient.Client {
	return tagclient.NewClusterClient(healthcheck.NoopFailed(hostlist.Fixture(addr)), nil)
}

func TestHealth(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/health", addr))
	defer resp.Body.Close()
	require.NoError(err)
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(err)
	require.Equal("OK\n", string(b))
}

func TestPut(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	tag := core.TagFixture()
	digest := core.DigestFixture()
	neighborClient := mocktagclient.NewMockClient(mocks.ctrl)

	mocks.depResolver.EXPECT().Resolve(tag, digest).Return(core.DigestList{digest}, nil)
	mocks.originClient.EXPECT().Stat(tag, digest).Return(core.NewBlobInfo(256), nil)
	mocks.store.EXPECT().Put(tag, digest, time.Duration(0)).Return(nil)
	mocks.provider.EXPECT().Provide(_testNeighbor).Return(neighborClient)
	neighborClient.EXPECT().DuplicatePut(
		tag, digest, mocks.config.DuplicateReplicateStagger).Return(nil)

	require.NoError(client.Put(tag, digest))
}

func TestPutInvalidParam(t *testing.T) {
	tag := core.TagFixture()
	digest := core.DigestFixture()

	tests := []struct {
		desc   string
		path   string
		status int
	}{
		{
			"empty tag",
			fmt.Sprintf("tags//digest/%s", digest),
			http.StatusBadRequest,
		}, {
			"invalid digest",
			fmt.Sprintf("tags/%s/digest/foo", url.PathEscape(tag)),
			http.StatusBadRequest,
		}, {
			"invalid replicate param",
			fmt.Sprintf("tags/%s/digest/%s?replicate=bar", url.PathEscape(tag), digest),
			http.StatusInternalServerError,
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			mocks, cleanup := newServerMocks(t)
			defer cleanup()

			addr, stop := testutil.StartServer(mocks.handler())
			defer stop()

			_, err := httputil.Put(fmt.Sprintf("http://%s/%s", addr, test.path))
			require.Error(err)
			require.True(httputil.IsStatus(err, test.status))
		})
	}
}

func TestDuplicatePut(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.NewSingleClient(addr, nil)

	tag := core.TagFixture()
	digest := core.DigestFixture()
	delay := 5 * time.Minute

	mocks.store.EXPECT().Put(tag, digest, delay).Return(nil)

	require.NoError(client.DuplicatePut(tag, digest, delay))
}

func TestDuplicatePutInvalidParam(t *testing.T) {
	tag := core.TagFixture()
	digest := core.DigestFixture()

	tests := []struct {
		desc   string
		path   string
		status int
	}{
		{
			"empty tag",
			fmt.Sprintf("internal/duplicate/tags//digest/%s", digest),
			http.StatusBadRequest,
		}, {
			"invalid digest",
			fmt.Sprintf("internal/duplicate/tags/%s/digest/foo", url.PathEscape(tag)),
			http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			mocks, cleanup := newServerMocks(t)
			defer cleanup()

			addr, stop := testutil.StartServer(mocks.handler())
			defer stop()

			_, err := httputil.Put(fmt.Sprintf("http://%s/%s", addr, test.path))
			require.Error(err)
			require.True(httputil.IsStatus(err, test.status))
		})
	}
}

func TestGet(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	tag := core.TagFixture()
	digest := core.DigestFixture()

	mocks.store.EXPECT().Get(tag).Return(digest, nil)

	result, err := client.Get(tag)
	require.NoError(err)
	require.Equal(digest, result)
}

func TestGetTagNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	tag := core.TagFixture()

	mocks.store.EXPECT().Get(tag).Return(core.Digest{}, tagstore.ErrTagNotFound)

	_, err := client.Get(tag)
	require.Equal(tagclient.ErrTagNotFound, err)
}

func TestHas(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	tag := core.TagFixture()
	digest := core.DigestFixture()

	mocks.backendClient.EXPECT().Stat(tag, tag).Return(core.NewBlobInfo(int64(len(digest.String()))), nil)

	ok, err := client.Has(tag)
	require.NoError(err)
	require.True(ok)
}

func TestHasNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	tag := core.TagFixture()

	mocks.backendClient.EXPECT().Stat(tag, tag).Return(nil, backenderrors.ErrBlobNotFound)

	ok, err := client.Has(tag)
	require.NoError(err)
	require.False(ok)
}

func TestListRepository(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	maxKeys := 3
	repo := "namespace-foo/repo-bar"
	tags := []string{"latest"}
	names := []string{fmt.Sprintf("%s:%s", repo, tags[0])}
	for i := 1; i < maxKeys*3; i++ {
		tags = append(tags, fmt.Sprintf("00%s", strconv.Itoa(i)))
		names = append(names, fmt.Sprintf("%s:%s", repo, tags[i]))
	}

	mocks.backendClient.EXPECT().List(repo+"/_manifests/tags").Return(&backend.ListResult{
		Names:             names[:maxKeys],
		ContinuationToken: "first",
	}, nil)

	// Func values are deeply equal if both are nil; otherwise they are not deeply
	// equal. So gomock.Any().
	mocks.backendClient.EXPECT().List(repo+"/_manifests/tags",
		gomock.Any()).Return(&backend.ListResult{
		Names:             names[maxKeys : maxKeys*2],
		ContinuationToken: "second",
	}, nil)

	mocks.backendClient.EXPECT().List(repo+"/_manifests/tags",
		gomock.Any()).Return(&backend.ListResult{
		Names: names[maxKeys*2:],
	}, nil)

	result, err := client.ListRepository(repo)
	require.NoError(err)
	require.Equal(tags, result)
}

func TestList(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	maxKeys := 3
	prefix := "namespace-foo/repo-bar/_manifests/tags"
	names := []string{"latest"}
	for i := 1; i < maxKeys*3; i++ {
		names = append(names, fmt.Sprintf("00%s", strconv.Itoa(i)))
	}

	mocks.backendClient.EXPECT().List(prefix).Return(&backend.ListResult{
		Names:             names[:maxKeys],
		ContinuationToken: "first",
	}, nil)

	mocks.backendClient.EXPECT().List(prefix,
		gomock.Any()).Return(&backend.ListResult{
		Names:             names[maxKeys : maxKeys*2],
		ContinuationToken: "second",
	}, nil)

	mocks.backendClient.EXPECT().List(prefix,
		gomock.Any()).Return(&backend.ListResult{
		Names: names[maxKeys*2:],
	}, nil)

	result, err := client.List(prefix)
	require.NoError(err)
	require.Equal(names, result)
}

func TestListEmptyPrefix(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	names := []string{"a", "b", "c"}

	mocks.backendClient.EXPECT().List("").Return(&backend.ListResult{
		Names: names,
	}, nil)

	result, err := client.List("")
	require.NoError(err)
	require.Equal(names, result)
}

func TestPutAndReplicate(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	tag := core.TagFixture()
	digest := core.DigestFixture()
	deps := core.DigestList{digest}
	neighborClient := mocktagclient.NewMockClient(mocks.ctrl)
	task := tagreplication.NewTask(tag, digest, deps, _testRemote, 0)
	replicaClient := mocks.client()

	gomock.InOrder(
		mocks.depResolver.EXPECT().Resolve(tag, digest).Return(core.DigestList{digest}, nil),
		mocks.originClient.EXPECT().Stat(tag, digest).Return(core.NewBlobInfo(256), nil),
		mocks.store.EXPECT().Put(tag, digest, time.Duration(0)).Return(nil),
		mocks.provider.EXPECT().Provide(_testNeighbor).Return(neighborClient),
		neighborClient.EXPECT().DuplicatePut(
			tag, digest, mocks.config.DuplicateReplicateStagger).Return(nil),
		mocks.tagReplicationManager.EXPECT().Add(tagreplication.MatchTask(task)).Return(nil),
		mocks.provider.EXPECT().Provide(_testNeighbor).Return(replicaClient),
		replicaClient.EXPECT().DuplicateReplicate(
			tag, digest, deps, mocks.config.DuplicateReplicateStagger).Return(nil),
	)

	require.NoError(client.PutAndReplicate(tag, digest))
}

func TestReplicate(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	tag := core.TagFixture()
	digest := core.DigestFixture()
	deps := core.DigestList{digest}
	task := tagreplication.NewTask(tag, digest, deps, _testRemote, 0)
	replicaClient := mocks.client()

	gomock.InOrder(
		mocks.store.EXPECT().Get(tag).Return(digest, nil),
		mocks.depResolver.EXPECT().Resolve(tag, digest).Return(deps, nil),
		mocks.tagReplicationManager.EXPECT().Add(tagreplication.MatchTask(task)).Return(nil),
		mocks.provider.EXPECT().Provide(_testNeighbor).Return(replicaClient),
		replicaClient.EXPECT().DuplicateReplicate(
			tag, digest, deps, mocks.config.DuplicateReplicateStagger).Return(nil),
	)

	require.NoError(client.Replicate(tag))
}

func TestReplicateNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	tag := core.TagFixture()

	gomock.InOrder(
		mocks.store.EXPECT().Get(tag).Return(core.Digest{}, tagstore.ErrTagNotFound),
	)

	err := client.Replicate(tag)
	require.Error(err)
	require.True(httputil.IsNotFound(err))
}

func TestDuplicateReplicate(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.NewSingleClient(addr, nil)

	tag := core.TagFixture()
	digest := core.DigestFixture()
	dependencies := core.DigestListFixture(3)
	delay := 5 * time.Minute
	task := tagreplication.NewTask(tag, digest, dependencies, _testRemote, delay)

	mocks.tagReplicationManager.EXPECT().Add(tagreplication.MatchTask(task)).Return(nil)

	require.NoError(client.DuplicateReplicate(tag, digest, dependencies, delay))
}

func TestDuplicateReplicateInvalidParam(t *testing.T) {
	tag := core.TagFixture()
	digest := core.DigestFixture()

	tests := []struct {
		desc   string
		path   string
		status int
	}{
		{
			"empty tag",
			fmt.Sprintf("internal/duplicate/remotes/tags//digest/%s", digest),
			http.StatusBadRequest,
		}, {
			"invalid digest",
			fmt.Sprintf("internal/duplicate/remotes/tags/%s/digest/foo", url.PathEscape(tag)),
			http.StatusInternalServerError,
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			mocks, cleanup := newServerMocks(t)
			defer cleanup()

			addr, stop := testutil.StartServer(mocks.handler())
			defer stop()

			_, err := httputil.Post(fmt.Sprintf("http://%s/%s", addr, test.path))
			require.Error(err)
			require.True(httputil.IsStatus(err, test.status))
		})
	}
}

func TestNoopReplicate(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	emptyRemotes, err := tagreplication.RemotesConfig{}.Build()
	require.NoError(err)

	mocks.remotes = emptyRemotes

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	tag := core.TagFixture()
	digest := core.DigestFixture()
	deps := core.DigestList{digest}

	gomock.InOrder(
		mocks.store.EXPECT().Get(tag).Return(digest, nil),
		mocks.depResolver.EXPECT().Resolve(tag, digest).Return(deps, nil),
	)

	// No replication tasks added or duplicated because no remotes are configured.

	require.NoError(client.Replicate(tag))
}

func TestOrigin(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := newClusterClient(addr)

	result, err := client.Origin()
	require.NoError(err)
	require.Equal(_testOrigin, result)
}
