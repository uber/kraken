package service

import (
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/golang/mock/gomock"
	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func startMetaInfoServer(h *metaInfoHandler) (addr string, stop func()) {
	r := chi.NewRouter()
	h.setRoutes(r, tally.NewTestScope("testing", nil))
	return testutil.StartServer(r)
}

func TestMetaInfoHandlerGetFetchesFromOrigin(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClusterClient := mockblobclient.NewMockClusterClient(ctrl)

	h := newMetaInfoHandler(
		MetaInfoConfig{}, storage.TestMetaInfoStore(), mockClusterClient)
	addr, stop := startMetaInfoServer(h)
	defer stop()

	mic := metainfoclient.Default(serverset.NewSingle(addr))

	namespace := "test-namespace"
	mi := torlib.MetaInfoFixture()
	digest := image.NewSHA256DigestFromHex(mi.Name())

	mockClusterClient.EXPECT().GetMetaInfo(namespace, digest).Return(mi, nil)

	_, err := mic.Download(namespace, digest.Hex())
	require.Equal(metainfoclient.ErrRetry, err)

	var result *torlib.MetaInfo
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		result, err = mic.Download(namespace, digest.Hex())
		return err == nil
	}))
	require.Equal(mi, result)
}

func TestMetaInfoHandlerGetCachesAndPropagatesOriginError(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClusterClient := mockblobclient.NewMockClusterClient(ctrl)

	h := newMetaInfoHandler(
		MetaInfoConfig{}, storage.TestMetaInfoStore(), mockClusterClient)
	addr, stop := startMetaInfoServer(h)
	defer stop()

	mic := metainfoclient.Default(serverset.NewSingle(addr))

	namespace := "test-namespace"
	mi := torlib.MetaInfoFixture()
	digest := image.NewSHA256DigestFromHex(mi.Name())

	mockClusterClient.EXPECT().GetMetaInfo(namespace, digest).Return(
		nil, httputil.StatusError{Status: 599})

	_, err := mic.Download(namespace, digest.Hex())
	require.Equal(metainfoclient.ErrRetry, err)

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := mic.Download(namespace, digest.Hex())
		return httputil.IsStatus(err, 599)
	}))
}
