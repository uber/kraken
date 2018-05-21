package tagserver

import (
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/mocks/lib/persistedretry"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	_testNamespace = "uber-usi/.*"
	_testOrigin    = "some-dns-record"
	_testRemote    = "remote-build-index"
)

type serverMocks struct {
	config                Config
	backends              *backend.Manager
	backendClient         *mockbackend.MockClient
	remotes               tagreplication.Remotes
	tagReplicationManager *mockpersistedretry.MockManager
}

func newServerMocks(t *testing.T) (*serverMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	backendClient := mockbackend.NewMockClient(ctrl)
	tagReplicationManager := mockpersistedretry.NewMockManager(ctrl)
	backends := backend.ManagerFixture()
	require.NoError(t, backends.Register(_testNamespace, backendClient))

	remotes, err := tagreplication.RemotesConfig{_testNamespace: []string{_testRemote}}.Build()
	if err != nil {
		panic(err)
	}

	return &serverMocks{Config{}, backends, backendClient, remotes, tagReplicationManager}, cleanup.Run
}

func (m *serverMocks) handler() http.Handler {
	return New(m.config, tally.NoopScope, m.backends, _testOrigin, m.remotes, m.tagReplicationManager).Handler()
}

func TestPutAndGetTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(addr)

	tag := "uber-usi/labrat"
	digest := core.DigestFixture()

	mocks.backendClient.EXPECT().Upload(tag, rwutil.MatchReader([]byte(digest.String()))).Return(nil)

	require.NoError(client.Put(tag, digest))

	mocks.backendClient.EXPECT().Download(tag, rwutil.MatchWriter([]byte(digest.String()))).Return(nil)

	// Getting tag multiple times should only make one download call.
	for i := 0; i < 10; i++ {
		result, err := client.Get(tag)
		require.NoError(err)
		require.Equal(digest, result)
	}
}

func TestGetTagNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(addr)

	tag := "uber-usi/labrat"

	mocks.backendClient.EXPECT().Download(tag, gomock.Any()).Return(backenderrors.ErrBlobNotFound)

	_, err := client.Get(tag)
	require.Equal(tagclient.ErrNotFound, err)
}

func TestReplicate(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(addr)

	tag := "uber-usi/labrat"
	digest := core.DigestFixture()
	dependencies := core.DigestListFixture(3)
	task := tagreplication.NewTask(tag, digest, dependencies, _testRemote)

	mocks.tagReplicationManager.EXPECT().Add(tagreplication.MatchTask(task)).Return(nil)

	require.NoError(client.Replicate(tag, digest, dependencies))
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
