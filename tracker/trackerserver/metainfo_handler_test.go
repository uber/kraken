package trackerserver

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/golang/mock/gomock"
	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const namespace = "test-namespace"

func download(addr string, d core.Digest) (*http.Response, error) {
	return httputil.Get(
		fmt.Sprintf("http://%s/namespace/%s/blobs/%s/metainfo", addr, namespace, d))
}

func startMetaInfoServer(h *metaInfoHandler) (addr string, stop func()) {
	r := chi.NewRouter()
	h.setRoutes(r)
	return testutil.StartServer(r)
}

func TestMetaInfoHandlerGetFetchesFromOrigin(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClusterClient := mockblobclient.NewMockClusterClient(ctrl)

	h := newMetaInfoHandler(
		Config{}, tally.NoopScope, storage.TestMetaInfoStore(), mockClusterClient)
	addr, stop := startMetaInfoServer(h)
	defer stop()

	mi := core.MetaInfoFixture()
	digest := core.NewSHA256DigestFromHex(mi.Name())

	mockClusterClient.EXPECT().GetMetaInfo(namespace, digest).Return(mi, nil)

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

func TestMetaInfoHandlerGetCachesAndPropagatesOriginError(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClusterClient := mockblobclient.NewMockClusterClient(ctrl)

	h := newMetaInfoHandler(
		Config{}, tally.NoopScope, storage.TestMetaInfoStore(), mockClusterClient)
	addr, stop := startMetaInfoServer(h)
	defer stop()

	mi := core.MetaInfoFixture()
	digest := core.NewSHA256DigestFromHex(mi.Name())

	mockClusterClient.EXPECT().GetMetaInfo(namespace, digest).Return(
		nil, httputil.StatusError{Status: 599})

	resp, err := download(addr, digest)
	require.True(httputil.IsAccepted(err))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		resp, err = download(addr, digest)
		return httputil.IsStatus(err, 599)
	}))
}
