package trackerserver

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/stretchr/testify/require"
)

const namespace = "test-namespace"

func download(addr string, d core.Digest) (*http.Response, error) {
	return httputil.Get(
		fmt.Sprintf("http://%s/namespace/%s/blobs/%s/metainfo", addr, namespace, d))
}

func TestGetMetaInfoHandlerFetchesFromOrigin(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	mi := core.MetaInfoFixture()
	digest := core.NewSHA256DigestFromHex(mi.Name())

	mocks.originCluster.EXPECT().GetMetaInfo(namespace, digest).Return(mi, nil)

	resp, err := download(addr, digest)
	require.True(httputil.IsAccepted(err))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		resp, err = download(addr, digest)
		return err == nil
	}))
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(err)
	result, err := core.DeserializeMetaInfo(b)
	require.NoError(err)
	require.Equal(mi, result)
}

func TestGetMetaInfoHandlerCachesAndPropagatesOriginError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	mi := core.MetaInfoFixture()
	digest := core.NewSHA256DigestFromHex(mi.Name())

	mocks.originCluster.EXPECT().GetMetaInfo(namespace, digest).Return(
		nil, httputil.StatusError{Status: 599})

	_, err := download(addr, digest)
	require.True(httputil.IsAccepted(err))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err = download(addr, digest)
		return httputil.IsStatus(err, 599)
	}))
}
