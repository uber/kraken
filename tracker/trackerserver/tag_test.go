package trackerserver

import (
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/tracker/tagclient"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestGetTagHandlerCachesResults(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(serverset.MustRoundRobin(addr))

	name := "some/repo:tag"
	value := core.DigestFixture().String()

	mocks.tags.EXPECT().Download(name, rwutil.MatchWriter([]byte(value))).Return(nil)

	// Getting name multiple times should only make one download call.
	for i := 0; i < 10; i++ {
		res, err := client.Get(name)
		require.NoError(err)
		require.Equal(value, res)
	}
}

func TestGetTagHandlerNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	client := tagclient.New(serverset.MustRoundRobin(addr))

	name := "some/repo:tag"

	mocks.tags.EXPECT().Download(name, gomock.Any()).Return(backenderrors.ErrBlobNotFound)

	for i := 0; i < 10; i++ {
		_, err := client.Get(name)
		require.Equal(tagclient.ErrNotFound, err)
	}
}
