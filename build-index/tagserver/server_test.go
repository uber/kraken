package tagserver

import (
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/mocks/build-index/remotes"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const _testNamespace = "uber-usi/.*"

const _testOrigin = "some-dns-record"

type serverMocks struct {
	config        Config
	backends      *backend.Manager
	backendClient *mockbackend.MockClient
	replicator    *mockremotes.MockReplicator
}

func newServerMocks(t *testing.T) (*serverMocks, func()) {
	ctrl := gomock.NewController(t)

	backendClient := mockbackend.NewMockClient(ctrl)
	backends := backend.ManagerFixture()

	require.NoError(t, backends.Register(_testNamespace, backendClient))

	replicator := mockremotes.NewMockReplicator(ctrl)

	return &serverMocks{Config{}, backends, backendClient, replicator}, ctrl.Finish
}

func (m *serverMocks) handler() http.Handler {
	return New(m.config, tally.NoopScope, m.backends, m.replicator, _testOrigin).Handler()
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

	mocks.backendClient.EXPECT().Upload(tag, rwutil.MatchReader([]byte(digest.Hex()))).Return(nil)

	require.NoError(client.Put(tag, digest))

	mocks.backendClient.EXPECT().Download(tag, rwutil.MatchWriter([]byte(digest.Hex()))).Return(nil)

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
	dependencies := []core.Digest{core.DigestFixture(), core.DigestFixture(), core.DigestFixture()}

	mocks.replicator.EXPECT().Replicate(tag, digest, dependencies)

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
