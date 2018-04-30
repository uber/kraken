package tagserver

import (
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const namespace = "uber-usi/.*"

type serverMocks struct {
	config        Config
	backends      *backend.Manager
	backendClient *mockbackend.MockClient
}

func newServerMocks(t *testing.T) (*serverMocks, func()) {
	ctrl := gomock.NewController(t)

	backendClient := mockbackend.NewMockClient(ctrl)

	backends, err := backend.NewManager(nil, nil)
	require.NoError(t, err)

	require.NoError(t, backends.Register(namespace, backendClient))

	return &serverMocks{Config{}, backends, backendClient}, ctrl.Finish
}

func (m *serverMocks) handler() http.Handler {
	return New(m.config, tally.NoopScope, m.backends).Handler()
}

func TestPutAndGetTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(serverset.MustRoundRobin(addr))

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

	client := tagclient.New(serverset.MustRoundRobin(addr))

	tag := "uber-usi/labrat"

	mocks.backendClient.EXPECT().Download(tag, gomock.Any()).Return(backenderrors.ErrBlobNotFound)

	_, err := client.Get(tag)
	require.Equal(tagclient.ErrNotFound, err)
}
