package transfer

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/utils/rwutil"
)

func TestRemoteBackendTransfererDownloadCachesBlobs(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	mockBlobBackendClient := mockbackend.NewMockClient(ctrl)
	mockTagBackendClient := mockbackend.NewMockClient(ctrl)
	rbt, err := NewRemoteBackendTransferer(mockTagBackendClient, mockBlobBackendClient, fs)
	require.NoError(err)

	d, blob := core.DigestWithBlobFixture()

	mockBlobBackendClient.EXPECT().Download(d.Hex(), rwutil.MatchWriter(blob)).Return(nil)

	_, err = rbt.Download(d.Hex())
	require.NoError(err)

	// Downloading again should use the cache (i.e. the mock should only be called once).
	r, err := rbt.Download(d.Hex())
	require.NoError(err)
	result, err := ioutil.ReadAll(r)
	require.NoError(err)
	require.Equal(string(blob), string(result))
}

func TestRemoteBackendTransfererDownloadCachesTags(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	mockBlobBackendClient := mockbackend.NewMockClient(ctrl)
	mockTagBackendClient := mockbackend.NewMockClient(ctrl)
	rbt, err := NewRemoteBackendTransferer(mockTagBackendClient, mockBlobBackendClient, fs)
	require.NoError(err)

	repo := "test_repo"
	tag := "test_tag"
	manifestDigest := core.DigestFixture()

	mockTagBackendClient.EXPECT().Download(
		fmt.Sprintf("%s:%s", repo, tag),
		backend.MatchWriter([]byte(manifestDigest.String()))).Return(nil)

	_, err = rbt.GetTag(repo, tag)
	require.NoError(err)

	// Downloading again should use the cache.
	d, err := rbt.GetTag(repo, tag)
	require.NoError(err)
	require.Equal(manifestDigest, d)
}

func TestRemoteBackendTransfererUploadBlobs(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	mockBlobBackendClient := mockbackend.NewMockClient(ctrl)
	mockTagBackendClient := mockbackend.NewMockClient(ctrl)

	d, blob := core.DigestWithBlobFixture()

	fs.CreateCacheFile(d.Hex(), bytes.NewReader(blob))

	rbt, err := NewRemoteBackendTransferer(mockTagBackendClient, mockBlobBackendClient, fs)
	require.NoError(err)

	mockBlobBackendClient.EXPECT().Upload(d.Hex(), rwutil.MatchReader(blob)).Return(nil)

	reader, err := fs.GetCacheFileReader(d.Hex())
	require.NoError(err)

	err = rbt.Upload(d.Hex(), reader, int64(len(blob)))
	require.NoError(err)
}
