package trackerserver

import (
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/hashring"
	"github.com/uber/kraken/tracker/metainfoclient"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/stretchr/testify/require"
)

func newMetaInfoClient(addr string) metainfoclient.Client {
	return metainfoclient.New(hashring.Fixture(addr), nil)
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
