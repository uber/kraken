package transfer

import (
	"bytes"
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
	fs            store.FileStore
}

func newProxyTransfererMocks(t *testing.T) (*proxyTransfererMocks, func()) {
	var cleanup testutil.Cleanup

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	tags := mocktagclient.NewMockClient(ctrl)

	originCluster := mockblobclient.NewMockClusterClient(ctrl)

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	return &proxyTransfererMocks{tags, originCluster, fs}, cleanup.Run
}

func (m *proxyTransfererMocks) new() *ProxyTransferer {
	return NewProxyTransferer(m.tags, m.originCluster, m.fs)
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

func TestPostTag(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newProxyTransfererMocks(t)
	defer cleanup()

	transferer := mocks.new()

	config := core.DigestFixture()
	layer1 := core.DigestFixture()
	layer2 := core.DigestFixture()

	manifestDigest, rawManifest := dockerutil.ManifestFixture(config, layer1, layer2)

	require.NoError(mocks.fs.CreateCacheFile(manifestDigest.Hex(), bytes.NewReader(rawManifest)))

	tag := "docker/some-tag"
	dependencies := []core.Digest{config, layer1, layer2, manifestDigest}

	gomock.InOrder(
		mocks.tags.EXPECT().Put(tag, manifestDigest).Return(nil),
		mocks.tags.EXPECT().Replicate(tag, manifestDigest, dependencies).Return(nil),
	)

	require.NoError(transferer.PostTag(tag, manifestDigest))
}
