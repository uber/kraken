package transfer

import (
	"bytes"
	"errors"
	"io/ioutil"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/build-index/tagclient"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/dockerutil"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type proxyTransfererMocks struct {
	tags          *mocktagclient.MockClient
	originCluster *mockblobclient.MockClusterClient
	cas           *store.CAStore
}

func newProxyTransfererMocks(t *testing.T) (*proxyTransfererMocks, func()) {
	var cleanup testutil.Cleanup

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	tags := mocktagclient.NewMockClient(ctrl)

	originCluster := mockblobclient.NewMockClusterClient(ctrl)

	cas, c := store.CAStoreFixture()
	cleanup.Add(c)

	return &proxyTransfererMocks{tags, originCluster, cas}, cleanup.Run
}

func (m *proxyTransfererMocks) new() *ProxyTransferer {
	return NewProxyTransferer(m.tags, m.originCluster, m.cas)
}

func TestProxyTransfererDownloadCachesBlob(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newProxyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/test-image"
	blob := core.NewBlobFixture()

	mocks.originCluster.EXPECT().DownloadBlob(
		namespace, blob.Digest, rwutil.MatchWriter(blob.Content)).Return(nil)

	// Downloading multiple times should only call blob download once.
	for i := 0; i < 10; i++ {
		result, err := transferer.Download(namespace, blob.Digest)
		require.NoError(err)
		b, err := ioutil.ReadAll(result)
		require.NoError(err)
		require.Equal(blob.Content, b)
	}
}

func TestProxyTransfererPostTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newProxyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	config := core.DigestFixture()
	layer1 := core.DigestFixture()
	layer2 := core.DigestFixture()

	manifestDigest, rawManifest := dockerutil.ManifestFixture(config, layer1, layer2)

	require.NoError(mocks.cas.CreateCacheFile(manifestDigest.Hex(), bytes.NewReader(rawManifest)))

	tag := "docker/some-tag"

	gomock.InOrder(
		mocks.tags.EXPECT().Put(tag, manifestDigest).Return(nil),
		mocks.tags.EXPECT().Replicate(tag).Return(nil),
	)

	require.NoError(transferer.PostTag(tag, manifestDigest))
}

func TestProxyTransfererStatLocalBlob(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newProxyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/test-image"
	blob := core.NewBlobFixture()

	require.NoError(mocks.cas.CreateCacheFile(blob.Digest.Hex(), bytes.NewReader(blob.Content)))

	bi, err := transferer.Stat(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(blob.Info(), bi)
}

func TestProxyTransfererStatRemoteBlob(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newProxyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/test-image"
	blob := core.NewBlobFixture()

	mocks.originCluster.EXPECT().Stat(namespace, blob.Digest).Return(blob.Info(), nil)

	bi, err := transferer.Stat(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(blob.Info(), bi)
}

func TestProxyTransfererStatNotFoundOnAnyOriginError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newProxyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/test-image"
	blob := core.NewBlobFixture()

	mocks.originCluster.EXPECT().Stat(namespace, blob.Digest).Return(nil, errors.New("any error"))

	_, err := transferer.Stat(namespace, blob.Digest)
	require.Equal(ErrBlobNotFound, err)
}
