package tagserver

import (
	"net/http"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/backend/blobinfo"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/mocks/build-index/tagclient"
	"code.uber.internal/infra/kraken/mocks/build-index/tagtype"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/mocks/lib/persistedretry"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/stringset"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	_testNamespace    = ".*"
	_testOrigin       = "some-dns-record"
	_testRemote       = "remote-build-index"
	_testLocalReplica = "local-build-index"
)

type serverMocks struct {
	ctrl                  *gomock.Controller
	config                Config
	backends              *backend.Manager
	backendClient         *mockbackend.MockClient
	remotes               tagreplication.Remotes
	tagReplicationManager *mockpersistedretry.MockManager
	provider              *mocktagclient.MockProvider
	tagTypes              *mocktagtype.MockManager
	originClient          *mockblobclient.MockClusterClient
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
		_testNamespace: []string{_testRemote},
	}.Build()
	if err != nil {
		panic(err)
	}

	provider := mocktagclient.NewMockProvider(ctrl)

	originClient := mockblobclient.NewMockClusterClient(ctrl)
	tagTypes := mocktagtype.NewMockManager(ctrl)

	return &serverMocks{
		ctrl:                  ctrl,
		config:                Config{DuplicateReplicateStagger: 20 * time.Minute},
		backends:              backends,
		backendClient:         backendClient,
		remotes:               remotes,
		tagReplicationManager: tagReplicationManager,
		provider:              provider,
		originClient:          originClient,
		tagTypes:              tagTypes,
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
		stringset.FromSlice([]string{_testLocalReplica}),
		m.remotes,
		m.tagReplicationManager,
		m.provider,
		m.tagTypes).Handler()
}

func TestPutAndGetLocalTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(addr)

	tag := core.TagFixture()
	digest := core.DigestFixture()
	tagDependencyResolver := mocktagtype.NewMockDependencyResolver(mocks.ctrl)

	mocks.tagTypes.EXPECT().GetDependencyResolver(tag).Return(tagDependencyResolver, nil)
	tagDependencyResolver.EXPECT().Resolve(tag, digest).Return(core.DigestList{digest}, nil)
	mocks.originClient.EXPECT().CheckBlob(tag, digest).Return(true, nil)
	mocks.backendClient.EXPECT().Upload(tag, rwutil.MatchReader([]byte(digest.String()))).Return(nil)

	require.NoError(client.Put(tag, digest))

	mocks.backendClient.EXPECT().Download(tag, rwutil.MatchWriter([]byte(digest.String()))).Return(nil)

	// Getting tag multiple times should only make one download call.
	for i := 0; i < 10; i++ {
		result, err := client.GetLocal(tag)
		require.NoError(err)
		require.Equal(digest, result)
	}
}

func TestGetTagFallback(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(addr)

	tag := core.TagFixture()
	digest := core.DigestFixture()
	remoteClient := mocks.client()

	gomock.InOrder(
		mocks.backendClient.EXPECT().Download(tag, gomock.Any()).Return(backenderrors.ErrBlobNotFound),
		mocks.provider.EXPECT().Provide(_testRemote).Return(remoteClient),
		remoteClient.EXPECT().GetLocal(tag).Return(digest, nil),
	)

	d, err := client.Get(tag)
	require.NoError(err)
	require.Equal(digest, d)
}

func TestGetTagNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(addr)

	tag := core.TagFixture()
	remoteClient := mocks.client()

	gomock.InOrder(
		mocks.backendClient.EXPECT().Download(tag, gomock.Any()).Return(backenderrors.ErrBlobNotFound),
		mocks.provider.EXPECT().Provide(_testRemote).Return(remoteClient),
		remoteClient.EXPECT().GetLocal(tag).Return(core.Digest{}, tagclient.ErrTagNotFound),
	)

	_, err := client.Get(tag)
	require.Equal(tagclient.ErrTagNotFound, err)
}

func TestHasTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(addr)

	tag := core.TagFixture()
	digest := core.DigestFixture()

	mocks.backendClient.EXPECT().Stat(tag).Return(blobinfo.New(int64(len(digest.String()))), nil)

	ok, err := client.Has(tag)
	require.NoError(err)
	require.True(ok)
}

func TestHasTagNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(addr)

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

	client := tagclient.New(addr)

	repo := "uber-usi/labrat"
	tags := []string{"latest", "0000", "0001"}

	mocks.backendClient.EXPECT().List(repo).Return(tags, nil)

	result, err := client.ListRepository(repo)
	require.NoError(err)
	require.Equal(tags, result)
}

func TestListRepositoryNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(addr)

	repo := "uber-usi/labrat"

	mocks.backendClient.EXPECT().List(repo).Return(nil, backenderrors.ErrDirNotFound)

	_, err := client.ListRepository(repo)
	require.Equal(tagclient.ErrRepoNotFound, err)
}

func TestReplicate(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(addr)

	tag := core.TagFixture()
	digest := core.DigestFixture()
	deps := core.DigestList{digest}
	task := tagreplication.NewTask(tag, digest, deps, _testRemote)
	replicaClient := mocks.client()
	tagDependencyResolver := mocktagtype.NewMockDependencyResolver(mocks.ctrl)

	gomock.InOrder(
		mocks.tagTypes.EXPECT().GetDependencyResolver(tag).Return(tagDependencyResolver, nil),
		mocks.backendClient.EXPECT().Download(tag, rwutil.MatchWriter([]byte(digest.String()))).Return(nil),
		tagDependencyResolver.EXPECT().Resolve(tag, digest).Return(core.DigestList{digest}, nil),
		mocks.tagReplicationManager.EXPECT().Add(tagreplication.MatchTask(task)).Return(nil),
		mocks.provider.EXPECT().Provide(_testLocalReplica).Return(replicaClient),
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

	client := tagclient.New(addr)

	tag := core.TagFixture()
	digest := core.DigestFixture()
	dependencies := core.DigestListFixture(3)
	delay := 5 * time.Minute
	task := tagreplication.NewTaskWithDelay(tag, digest, dependencies, _testRemote, delay)

	mocks.tagReplicationManager.EXPECT().Add(tagreplication.MatchTask(task)).Return(nil)

	require.NoError(client.DuplicateReplicate(tag, digest, dependencies, delay))
}

func TestOrigin(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(addr)

	result, err := client.Origin()
	require.NoError(err)
	require.Equal(_testOrigin, result)
}
