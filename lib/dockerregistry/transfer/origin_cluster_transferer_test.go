package transfer

import (
	"errors"
	"fmt"
	"io/ioutil"
	"sort"
	"testing"

	"github.com/golang/mock/gomock"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/stretchr/testify/require"
)

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

	mocks.blobClientProvider.EXPECT().Provide(transferer.originAddr).Return(mocks.blobClients[transferer.originAddr])
	mocks.blobClients[transferer.originAddr].EXPECT().Locations(digest).Return(locs, nil)
	mocks.blobClientProvider.EXPECT().Provide("loc1").Return(mocks.blobClients["loc1"])
	mocks.blobClients["loc1"].EXPECT().GetBlob(digest).Return(r, nil)

	readCloser, err := transferer.Download(digest.Hex())
	require.NoError(err)
	defer readCloser.Close()
	data, err := ioutil.ReadAll(readCloser)
	require.NoError(err)
	require.Equal(blob, data)
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

	mocks.blobClientProvider.EXPECT().Provide(transferer.originAddr).Return(mocks.blobClients[transferer.originAddr])
	mocks.blobClients[transferer.originAddr].EXPECT().Locations(digest).Return(locs, nil)
	mocks.blobClientProvider.EXPECT().Provide("loc1").Return(mocks.blobClients["loc1"])
	mocks.blobClients["loc1"].EXPECT().GetBlob(digest).Return(nil, errors.New("not found"))
	mocks.blobClientProvider.EXPECT().Provide("loc2").Return(mocks.blobClients["loc1"])
	mocks.blobClients["loc1"].EXPECT().GetBlob(digest).Return(r, nil)

	readCloser, err := transferer.Download(digest.Hex())
	require.NoError(err)
	defer readCloser.Close()
	data, err := ioutil.ReadAll(readCloser)
	require.NoError(err)
	require.Equal(blob, data)
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

	_, err = transferer.Download(digest.Hex())
	require.Equal(fmt.Errorf("failed to pull blob from all locations: failed to pull blob from loc1: not found, failed to pull blob from loc2: not found, failed to pull blob from loc3: not found"), err)
}

func TestUploadSuccessAll(t *testing.T) {
	require := require.New(t)
	locs := []string{"loc1", "loc2", "loc3"}
	mocks := newOrginClusterTransfererMocks(t, locs...)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	var size int64 = 256
	blob := randutil.Text(int(size))
	digest, err := image.NewDigester().FromBytes(blob)
	require.NoError(err)

	r, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()

	mocks.blobClientProvider.EXPECT().Provide(transferer.originAddr).Return(mocks.blobClients[transferer.originAddr])
	mocks.blobClients[transferer.originAddr].EXPECT().Locations(digest).Return(locs, nil).Times(2)
	for _, loc := range locs {
		mocks.blobClientProvider.EXPECT().Provide(loc).Return(mocks.blobClients[loc])
		mocks.blobClients[loc].EXPECT().PushBlob(digest, gomock.Any(), size).Return(nil)

	}

	require.NoError(transferer.Upload(digest.Hex(), r, size))
}

func TestUploadSuccessMajority(t *testing.T) {
	require := require.New(t)
	locs := []string{"loc1", "loc2", "loc3", "loc4"}
	mocks := newOrginClusterTransfererMocks(t, locs...)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	var size int64 = 256
	blob := randutil.Text(int(size))
	digest, err := image.NewDigester().FromBytes(blob)
	require.NoError(err)

	r, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()

	mocks.blobClientProvider.EXPECT().Provide(transferer.originAddr).Return(mocks.blobClients[transferer.originAddr])
	mocks.blobClients[transferer.originAddr].EXPECT().Locations(digest).Return(locs, nil).Times(2)
	// one failure
	mocks.blobClientProvider.EXPECT().Provide("loc1").Return(mocks.blobClients["loc1"])
	mocks.blobClients["loc1"].EXPECT().PushBlob(digest, gomock.Any(), size).Return(errors.New("loc1 503"))

	for _, loc := range locs[1:] {
		mocks.blobClientProvider.EXPECT().Provide(loc).Return(mocks.blobClients[loc])
		mocks.blobClients[loc].EXPECT().PushBlob(digest, gomock.Any(), size).Return(nil)
	}

	require.NoError(transferer.Upload(digest.Hex(), r, size))
}

func TestUploadFailureMajority(t *testing.T) {
	require := require.New(t)
	locs := []string{"loc1", "loc2", "loc3", "loc4"}
	mocks := newOrginClusterTransfererMocks(t, locs...)
	defer mocks.ctrl.Finish()
	transferer := testOriginClusterTransferer(mocks)

	var size int64 = 256
	blob := randutil.Text(int(size))
	digest, err := image.NewDigester().FromBytes(blob)
	require.NoError(err)

	r, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()

	mocks.blobClientProvider.EXPECT().Provide(transferer.originAddr).Return(mocks.blobClients[transferer.originAddr])
	mocks.blobClients[transferer.originAddr].EXPECT().Locations(digest).Return(locs, nil).Times(2)
	// two failures
	mocks.blobClientProvider.EXPECT().Provide("loc1").Return(mocks.blobClients["loc1"])
	mocks.blobClients["loc1"].EXPECT().PushBlob(digest, gomock.Any(), size).Return(errors.New("loc1 503"))
	mocks.blobClientProvider.EXPECT().Provide("loc2").Return(mocks.blobClients["loc2"])
	mocks.blobClients["loc2"].EXPECT().PushBlob(digest, gomock.Any(), size).Return(errors.New("loc2 503"))

	for _, loc := range locs[2:] {
		mocks.blobClientProvider.EXPECT().Provide(loc).Return(mocks.blobClients[loc])
		mocks.blobClients[loc].EXPECT().PushBlob(digest, gomock.Any(), size).Return(nil)
	}

	err = transferer.Upload(digest.Hex(), r, size)
	require.Error(err)
	var failedLocs []string
	for _, e := range err.(uploadQuorumError).errs {
		failedLocs = append(failedLocs, e.(pushBlobError).loc)
	}
	sort.Strings(failedLocs)
	require.Equal([]string{"loc1", "loc2"}, failedLocs)
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

	mocks.manifestClient.EXPECT().GetManifest(repo, tag).Return(rw, nil)

	readCloser, err := transferer.GetManifest(repo, tag)
	require.NoError(err)
	defer readCloser.Close()
	ok, err := image.Verify(digest, readCloser)
	require.NoError(err)
	require.True(ok)
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

	mocks.manifestClient.EXPECT().PostManifest(repo, tag, digest.Hex(), r).Return(nil)
	require.NoError(transferer.PostManifest(repo, tag, digest.Hex(), r))
}
