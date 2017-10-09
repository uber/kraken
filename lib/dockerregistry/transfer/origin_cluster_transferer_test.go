package transfer

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/stretchr/testify/require"
)

func TestSaveBlob(t *testing.T) {
	require := require.New(t)
	mocks := newOrginClusterTransfererMocks(t)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	blob := randutil.Text(256)
	digest, err := image.NewDigester().FromBytes(blob)
	require.NoError(err)

	rw, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()
	mocks.fileStore.EXPECT().CreateUploadFile(gomock.Any(), int64(0)).Return(nil)
	mocks.fileStore.EXPECT().GetUploadFileReadWriter(gomock.Any()).Return(rw, nil)
	mocks.fileStore.EXPECT().MoveUploadFileToCache(gomock.Any(), digest.Hex()).Return(nil)

	require.NoError(transferer.saveBlob(bytes.NewReader(blob), digest))
}

func TestDownloadSuccess(t *testing.T) {
	require := require.New(t)
	locs := []string{"loc1", "loc2", "loc3"}
	mocks := newOrginClusterTransfererMocks(t, locs...)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	blob := randutil.Text(256)
	digest, err := image.NewDigester().FromBytes(blob)
	require.NoError(err)

	r, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()

	w, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()

	mocks.blobClientProvider.EXPECT().Provide(transferer.originAddr).Return(mocks.blobClients[transferer.originAddr])
	mocks.blobClients[transferer.originAddr].EXPECT().Locations(digest).Return(locs, nil)
	mocks.blobClientProvider.EXPECT().Provide("loc1").Return(mocks.blobClients["loc1"])
	mocks.blobClients["loc1"].EXPECT().GetBlob(digest).Return(r, nil)

	mocks.fileStore.EXPECT().CreateUploadFile(gomock.Any(), int64(0)).Return(nil)
	mocks.fileStore.EXPECT().GetUploadFileReadWriter(gomock.Any()).Return(w, nil)
	mocks.fileStore.EXPECT().MoveUploadFileToCache(gomock.Any(), digest.Hex()).Return(nil)

	require.NoError(transferer.Download(digest.Hex()))
}

func TestDownloadRetrySuccess(t *testing.T) {
	require := require.New(t)
	locs := []string{"loc1", "loc2", "loc3"}
	mocks := newOrginClusterTransfererMocks(t, locs...)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	blob := randutil.Text(256)
	digest, err := image.NewDigester().FromBytes(blob)
	require.NoError(err)

	r, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()

	w, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()

	mocks.blobClientProvider.EXPECT().Provide(transferer.originAddr).Return(mocks.blobClients[transferer.originAddr])
	mocks.blobClients[transferer.originAddr].EXPECT().Locations(digest).Return(locs, nil)
	mocks.blobClientProvider.EXPECT().Provide("loc1").Return(mocks.blobClients["loc1"])
	mocks.blobClients["loc1"].EXPECT().GetBlob(digest).Return(nil, errors.New("not found"))
	mocks.blobClientProvider.EXPECT().Provide("loc2").Return(mocks.blobClients["loc1"])
	mocks.blobClients["loc1"].EXPECT().GetBlob(digest).Return(r, nil)

	mocks.fileStore.EXPECT().CreateUploadFile(gomock.Any(), int64(0)).Return(nil)
	mocks.fileStore.EXPECT().GetUploadFileReadWriter(gomock.Any()).Return(w, nil)
	mocks.fileStore.EXPECT().MoveUploadFileToCache(gomock.Any(), digest.Hex()).Return(nil)

	require.NoError(transferer.Download(digest.Hex()))
}

func TestDownloadRetryFailure(t *testing.T) {
	require := require.New(t)
	locs := []string{"loc1", "loc2", "loc3"}
	mocks := newOrginClusterTransfererMocks(t, locs...)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	blob := randutil.Text(256)
	digest, err := image.NewDigester().FromBytes(blob)
	require.NoError(err)

	mocks.blobClientProvider.EXPECT().Provide(transferer.originAddr).Return(mocks.blobClients[transferer.originAddr])
	mocks.blobClients[transferer.originAddr].EXPECT().Locations(digest).Return(locs, nil)
	for _, loc := range locs {
		mocks.blobClientProvider.EXPECT().Provide(loc).Return(mocks.blobClients[loc])
		mocks.blobClients[loc].EXPECT().GetBlob(digest).Return(nil, errors.New("not found"))
	}

	require.Equal(fmt.Errorf("failed to pull blob from all locations: failed to pull blob from loc1: not found, failed to pull blob from loc2: not found, failed to pull blob from loc3: not found"), transferer.Download(digest.Hex()))
}

func TestUploadSuccessAll(t *testing.T) {
	require := require.New(t)
	locs := []string{"loc1", "loc2", "loc3"}
	mocks := newOrginClusterTransfererMocks(t, locs...)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	blob := randutil.Text(256)
	digest, err := image.NewDigester().FromBytes(blob)
	require.NoError(err)

	mocks.blobClientProvider.EXPECT().Provide(transferer.originAddr).Return(mocks.blobClients[transferer.originAddr])
	mocks.blobClients[transferer.originAddr].EXPECT().Locations(digest).Return(locs, nil).Times(2)
	for _, loc := range locs {
		mocks.blobClientProvider.EXPECT().Provide(loc).Return(mocks.blobClients[loc])
		mocks.blobClients[loc].EXPECT().PushBlob(digest, gomock.Any()).Return(nil)

	}

	for i := 0; i < len(locs); i++ {
		r, cleanup := store.NewMockFileReadWriter(blob)
		defer cleanup()
		mocks.fileStore.EXPECT().GetCacheFileReader(digest.Hex()).Return(r, nil)
	}

	require.NoError(transferer.Upload(digest.Hex()))
}

func TestUploadSuccessMajority(t *testing.T) {
	require := require.New(t)
	locs := []string{"loc1", "loc2", "loc3", "loc4"}
	mocks := newOrginClusterTransfererMocks(t, locs...)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	blob := randutil.Text(256)
	digest, err := image.NewDigester().FromBytes(blob)
	require.NoError(err)

	mocks.blobClientProvider.EXPECT().Provide(transferer.originAddr).Return(mocks.blobClients[transferer.originAddr])
	mocks.blobClients[transferer.originAddr].EXPECT().Locations(digest).Return(locs, nil).Times(2)
	// one failure
	mocks.blobClientProvider.EXPECT().Provide("loc1").Return(mocks.blobClients["loc1"])
	mocks.blobClients["loc1"].EXPECT().PushBlob(digest, gomock.Any()).Return(errors.New("loc1 503"))

	for _, loc := range locs[1:] {
		mocks.blobClientProvider.EXPECT().Provide(loc).Return(mocks.blobClients[loc])
		mocks.blobClients[loc].EXPECT().PushBlob(digest, gomock.Any()).Return(nil)
	}

	for i := 0; i < len(locs); i++ {
		r, cleanup := store.NewMockFileReadWriter(blob)
		defer cleanup()
		mocks.fileStore.EXPECT().GetCacheFileReader(digest.Hex()).Return(r, nil)
	}

	require.NoError(transferer.Upload(digest.Hex()))
}

func TestUploadFailureMajority(t *testing.T) {
	require := require.New(t)
	locs := []string{"loc1", "loc2", "loc3", "loc4"}
	mocks := newOrginClusterTransfererMocks(t, locs...)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	blob := randutil.Text(256)
	digest, err := image.NewDigester().FromBytes(blob)
	require.NoError(err)

	mocks.blobClientProvider.EXPECT().Provide(transferer.originAddr).Return(mocks.blobClients[transferer.originAddr])
	mocks.blobClients[transferer.originAddr].EXPECT().Locations(digest).Return(locs, nil).Times(2)
	// two failures
	mocks.blobClientProvider.EXPECT().Provide("loc1").Return(mocks.blobClients["loc1"])
	mocks.blobClients["loc1"].EXPECT().PushBlob(digest, gomock.Any()).Return(errors.New("loc1 503"))
	mocks.blobClientProvider.EXPECT().Provide("loc2").Return(mocks.blobClients["loc2"])
	mocks.blobClients["loc2"].EXPECT().PushBlob(digest, gomock.Any()).Return(errors.New("loc2 503"))

	for _, loc := range locs[2:] {
		mocks.blobClientProvider.EXPECT().Provide(loc).Return(mocks.blobClients[loc])
		mocks.blobClients[loc].EXPECT().PushBlob(digest, gomock.Any()).Return(nil)
	}

	for i := 0; i < len(locs); i++ {
		r, cleanup := store.NewMockFileReadWriter(blob)
		defer cleanup()
		mocks.fileStore.EXPECT().GetCacheFileReader(digest.Hex()).Return(r, nil)
	}

	err = transferer.Upload(digest.Hex())
	require.Error(err)
	require.True(strings.Contains(err.Error(), "failed to push blob to loc2: loc2 503"))
	require.True(strings.Contains(err.Error(), "failed to push blob to loc1: loc1 503"))
}

func TestGetManifest(t *testing.T) {
	require := require.New(t)
	repo := "testrepo"
	tag := "testtag"
	mocks := newOrginClusterTransfererMocks(t)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	rw, digest, cleanup := mockManifestReadWriter()
	defer cleanup()

	w, cleanup := store.NewMockFileReadWriter([]byte{})
	defer cleanup()

	mocks.manifestClient.EXPECT().GetManifest(repo, tag).Return(rw, nil)
	mocks.fileStore.EXPECT().CreateUploadFile(gomock.Any(), int64(0)).Return(nil)
	mocks.fileStore.EXPECT().GetUploadFileReadWriter(gomock.Any()).Return(w, nil)
	mocks.fileStore.EXPECT().MoveUploadFileToCache(gomock.Any(), digest.Hex()).Return(nil)

	d, err := transferer.GetManifest(repo, tag)
	require.NoError(err)
	require.Equal(digest.Hex(), d)
}

func TestPostManifest(t *testing.T) {
	require := require.New(t)
	repo := "testrepo"
	tag := "testtag"
	mocks := newOrginClusterTransfererMocks(t)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	_, digest, cleanup := mockManifestReadWriter()
	defer cleanup()

	r, cleanup := store.NewMockFileReadWriter([]byte{})
	defer cleanup()

	mocks.fileStore.EXPECT().GetCacheFileReader(digest.Hex()).Return(r, nil)
	mocks.manifestClient.EXPECT().PostManifest(repo, tag, digest.Hex(), r).Return(nil)

	require.NoError(transferer.PostManifest(repo, tag, digest.Hex()))
}
