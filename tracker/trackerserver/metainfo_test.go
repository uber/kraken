package trackerserver

import (
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/backoff"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/randutil"
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

func TestGetMetaInfoHandlerCachesAndPropagatesOriginError(t *testing.T) {
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
		namespace, digest).Return(nil, httputil.StatusError{Status: 599})

	client := metainfoclient.New(addr)

	_, err = client.Download(namespace, digest.Hex())
	require.Error(err)
	require.True(httputil.IsStatus(err, 599))
}

func TestConcurrentGetMetaInfo(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{GetMetaInfoLimit: 100 * time.Millisecond})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	namespace := core.TagFixture()
	mi := core.MetaInfoFixture()
	digest, err := core.NewSHA256DigestFromHex(mi.Name())
	require.NoError(err)

	mocks.originCluster.EXPECT().GetMetaInfo(
		namespace, digest).Return(nil, httputil.StatusError{Status: 202}).Times(3)
	mocks.originCluster.EXPECT().GetMetaInfo(namespace, digest).Return(mi, nil)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(randutil.Duration(100 * time.Millisecond))
			client := metainfoclient.NewWithBackoff(addr, backoff.New(backoff.Config{
				Min: 10 * time.Millisecond,
				Max: 10 * time.Millisecond,
			}))
			result, err := client.Download(namespace, digest.Hex())
			require.NoError(err)
			require.Equal(mi, result)
		}()
	}
	wg.Wait()
}
