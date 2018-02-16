package transfer

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/mocks/lib/dockerregistry/transfer/manifestclient"
)

func TestRemoteBackendTransfererDownloadCachesBlobs(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	mockBackendClient := mockbackend.NewMockClient(ctrl)

	d, blob := core.DigestWithBlobFixture()

	rbt, err := NewRemoteBackendTransferer(mockmanifestclient.NewMockClient(ctrl), mockBackendClient, fs)
	require.NoError(err)

	mockBackendClient.EXPECT().Download(d.Hex(), backend.MatchWriter(blob)).Return(nil)

	_, err = rbt.Download(d.Hex())
	require.NoError(err)

	// Downloading again should use the cache (i.e. the mock should only be called once).
	r, err := rbt.Download(d.Hex())
	require.NoError(err)
	result, err := ioutil.ReadAll(r)
	require.NoError(err)
	require.Equal(string(blob), string(result))
}

func TestRemoteBackendTransfererUploadBlobs(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	mockBackendClient := mockbackend.NewMockClient(ctrl)

	d, blob := core.DigestWithBlobFixture()

	fs.CreateCacheFile(d.Hex(), bytes.NewReader(blob))

	rbt, err := NewRemoteBackendTransferer(mockmanifestclient.NewMockClient(ctrl), mockBackendClient, fs)
	require.NoError(err)

	mockBackendClient.EXPECT().Upload(d.Hex(), backend.MatchReader(blob)).Return(nil)

	reader, err := fs.GetCacheFileReader(d.Hex())
	require.NoError(err)

	err = rbt.Upload(d.Hex(), reader, int64(len(blob)))
	require.NoError(err)
}
