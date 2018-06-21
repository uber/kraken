package transfer

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/build-index/tagclient"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/dockerutil"
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

func TestProxyTransfererDownloadFail(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newProxyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	namespace := "docker/test-image"
	blob := core.NewBlobFixture()

	// Download would always fail.
	_, err := transferer.Download(namespace, blob.Digest)
	require.Error(err)
	require.Equal(ErrBlobNotFound, err)
}

func TestPostTag(t *testing.T) {
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
