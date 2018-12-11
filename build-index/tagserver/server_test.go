package tagserver

import (
	"net/http"
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
		panic(err)
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

	mocks.backendClient.EXPECT().Stat(tag).Return(core.NewBlobInfo(int64(len(digest.String()))), nil)

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

	mocks.backendClient.EXPECT().Stat(tag).Return(nil, backenderrors.ErrBlobNotFound)

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

	repo := "uber-usi/labrat"
	tags := []string{"latest", "0000", "0001"}

	var names []string
	for _, tag := range tags {
		names = append(names, repo+":"+tag)
	}

	mocks.backendClient.EXPECT().List(repo+"/_manifests/tags").Return(names, nil)

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

	prefix := "uber-usi/labrat/_manifests/tags"
	names := []string{"latest", "0000", "0001"}

	mocks.backendClient.EXPECT().List(prefix).Return(names, nil)

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

	mocks.backendClient.EXPECT().List("").Return(names, nil)

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
