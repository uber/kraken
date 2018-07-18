package trackerserver

import (
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/stretchr/testify/require"
)

func TestGetMetaInfoHandlerFetchesFromOrigin(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	namespace := core.TagFixture()
	mi := core.MetaInfoFixture()
	digest, err := core.NewSHA256DigestFromHex(mi.Name())
	require.NoError(err)

	mocks.originCluster.EXPECT().GetMetaInfo(namespace, digest).Return(mi, nil)

	client := metainfoclient.New(addr)

	result, err := client.Download(namespace, digest.Hex())
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
	digest, err := core.NewSHA256DigestFromHex(mi.Name())
	require.NoError(err)

	mocks.originCluster.EXPECT().GetMetaInfo(
		namespace, digest).Return(nil, httputil.StatusError{Status: 599}).MinTimes(1)

	client := metainfoclient.New(addr)

	_, err = client.Download(namespace, digest.Hex())
	require.Error(err)
	require.True(httputil.IsStatus(err, 599))
}
