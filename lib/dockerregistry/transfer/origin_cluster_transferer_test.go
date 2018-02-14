package transfer

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/dockerregistry/transfer/manifestclient"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
)

func TestOriginClusterTransfererDownloadCachesBlobs(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClusterClient := mockblobclient.NewMockClusterClient(ctrl)

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	oct := NewOriginClusterTransferer(mockClusterClient, mockmanifestclient.NewMockClient(ctrl), fs)

	d, blob := core.DigestWithBlobFixture()

	mockClusterClient.EXPECT().DownloadBlob(d).Return(ioutil.NopCloser(bytes.NewReader(blob)), nil)

	r, err := oct.Download(d.Hex())
	require.NoError(err)
	result, err := ioutil.ReadAll(r)
	require.NoError(err)
	require.Equal(string(blob), string(result))

	// Downloading again should use the cache (i.e. the mock should only be called once).
	r, err = oct.Download(d.Hex())
	require.NoError(err)
	result, err = ioutil.ReadAll(r)
	require.NoError(err)
	require.Equal(string(blob), string(result))
}
