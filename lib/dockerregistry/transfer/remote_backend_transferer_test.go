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

	blob := core.NewBlobFixture()

	mockBlobBackendClient.EXPECT().Download(blob.Digest.Hex(), rwutil.MatchWriter(blob.Content)).Return(nil)

	_, err = rbt.Download(blob.Digest.Hex())
	require.NoError(err)

	// Downloading again should use the cache (i.e. the mock should only be called once).
	r, err := rbt.Download(blob.Digest.Hex())
	require.NoError(err)
	result, err := ioutil.ReadAll(r)
	require.NoError(err)
	require.Equal(string(blob.Content), string(result))
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
		rwutil.MatchWriter([]byte(manifestDigest.String()))).Return(nil)

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

	blob := core.NewBlobFixture()

	fs.CreateCacheFile(blob.Digest.Hex(), bytes.NewReader(blob.Content))

	rbt, err := NewRemoteBackendTransferer(mockTagBackendClient, mockBlobBackendClient, fs)
	require.NoError(err)

	mockBlobBackendClient.EXPECT().Upload(blob.Digest.Hex(), rwutil.MatchReader(blob.Content)).Return(nil)

	reader, err := fs.GetCacheFileReader(blob.Digest.Hex())
	require.NoError(err)

	err = rbt.Upload(blob.Digest.Hex(), reader, int64(len(blob.Content)))
	require.NoError(err)
}
