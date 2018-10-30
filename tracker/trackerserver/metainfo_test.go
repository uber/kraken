package trackerserver

import (
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/stretchr/testify/require"
)

func newMetaInfoClient(addr string) metainfoclient.Client {
	return metainfoclient.New(healthcheck.NoopFailed(hostlist.Fixture(addr)), nil)
}

func TestGetMetaInfoHandlerFetchesFromOrigin(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	namespace := core.TagFixture()
	mi := core.MetaInfoFixture()

	mocks.originCluster.EXPECT().GetMetaInfo(namespace, mi.Digest()).Return(mi, nil)

	client := newMetaInfoClient(addr)

	result, err := client.Download(namespace, mi.Digest())
	require.NoError(err)
	require.Equal(mi, result)
}

func TestGetMetaInfoHandlerPropagatesOriginError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	namespace := core.TagFixture()
	mi := core.MetaInfoFixture()

	mocks.originCluster.EXPECT().GetMetaInfo(
		namespace, mi.Digest()).Return(nil, httputil.StatusError{Status: 599}).MinTimes(1)

	client := newMetaInfoClient(addr)

	_, err := client.Download(namespace, mi.Digest())
	require.Error(err)
	require.True(httputil.IsStatus(err, 599))
}
