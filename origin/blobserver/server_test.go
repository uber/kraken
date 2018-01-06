package blobserver

import (
	"bytes"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/fileio"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/testutil"
)

func TestCheckBlobHandlerOK(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configNoRedirectFixture())
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, nil)

	ok, err := blobclient.New(addr).CheckBlob(d)
	require.NoError(err)
	require.True(ok)
}

func TestCheckBlobHandlerNotFound(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configNoRedirectFixture())
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, os.ErrNotExist)

	ok, err := blobclient.New(addr).CheckBlob(d)
	require.NoError(err)
	require.False(ok)
}

func TestGetBlobHandlerOK(t *testing.T) {
	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configNoRedirectFixture())
	defer stop()

	d := image.DigestFixture()

	blob := randutil.Text(256)
	f, cleanup := store.NewMockFileReadWriter(blob)
	defer cleanup()

	mocks.fileStore.EXPECT().GetCacheFileReader(d.Hex()).Return(f, nil)

	ensureHasBlob(t, blobclient.New(addr), d, blob)
}

func TestGetBlobHandlerNotFound(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configNoRedirectFixture())
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileReader(d.Hex()).Return(nil, os.ErrNotExist)

	_, err := blobclient.New(addr).GetBlob(d)
	require.Error(err)
	require.Equal(http.StatusNotFound, err.(httputil.StatusError).Status)
}

func TestDeleteBlobHandlerAccepted(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configFixture())
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().MoveCacheFileToTrash(d.Hex()).Return(nil)

	require.NoError(blobclient.New(addr).DeleteBlob(d))
}

func TestDeleteBlobHandlerNotFound(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configFixture())
	defer stop()

	d := image.DigestFixture()

	mocks.fileStore.EXPECT().MoveCacheFileToTrash(d.Hex()).Return(os.ErrNotExist)

	err := blobclient.New(addr).DeleteBlob(d)
	require.Error(err)
	require.Equal(http.StatusNotFound, err.(httputil.StatusError).Status)
}

func TestGetLocationsHandlerOK(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	config := configFixture()

	addr, stop := mocks.server(config)
	defer stop()

	d, _ := computeBlobForHosts(config, master1, master2)

	locs, err := blobclient.New(addr).Locations(d)
	require.NoError(err)
	require.Equal([]string{master1, master2}, locs)
}

func TestRedirectErrors(t *testing.T) {
	config := configFixture()
	d, _ := computeBlobForHosts(config, master2, master3)

	tests := []struct {
		name string
		f    func(addr string) error
	}{
		{
			"CheckBlob",
			func(addr string) error { _, err := blobclient.New(addr).CheckBlob(d); return err },
		}, {
			"GetBlob",
			func(addr string) error { _, err := blobclient.New(addr).GetBlob(d); return err },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			mocks := newServerMocks(t)
			defer mocks.ctrl.Finish()

			addr, stop := mocks.server(config)
			defer stop()

			err := test.f(addr)
			require.Error(err)
			require.Equal([]string{master2, master3}, err.(blobclient.RedirectError).Locations)
		})
	}
}

func TestGetPeerContextHandlerOK(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(master1, configFixture(), cp)
	defer s.cleanup()

	pctx, err := cp.Provide(master1).GetPeerContext()
	require.NoError(err)
	require.Equal(s.pctx, pctx)
}

func TestGetMetaInfoHandlerDownloadsBlobAndReplicates(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := configFixture()
	cp := newTestClientProvider()
	namespace := "test-namespace"
	mockBackendClient := mockbackend.NewMockClient(ctrl)

	for _, master := range []string{master1, master2} {
		s := newTestServer(master, configFixture(), cp)
		defer s.cleanup()
		s.backendManager.Register(namespace, mockBackendClient)
	}

	d, blob := computeBlobForHosts(config, master1, master2)

	mockBackendClient.EXPECT().Download(d.Hex(), fileio.MatchWriter(blob)).Return(nil)

	mi, err := cp.Provide(master1).GetMetaInfo(namespace, d)
	require.True(httputil.IsAccepted(err))
	require.Nil(mi)

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := cp.Provide(master1).GetMetaInfo(namespace, d)
		return !httputil.IsAccepted(err)
	}))

	mi, err = cp.Provide(master1).GetMetaInfo(namespace, d)
	require.NoError(err)
	require.NotNil(mi)
	require.Equal(len(blob), int(mi.Info.Length))

	// Ensure blob was replicated to other master.
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		ok, err := cp.Provide(master2).CheckBlob(d)
		return ok && err == nil
	}))
}

func TestGetMetaInfoHandlerBlobNotFound(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	namespace := "test-namespace"
	mockBackendClient := mockbackend.NewMockClient(ctrl)

	cp := newTestClientProvider()

	s := newTestServer(master1, configFixture(), cp)
	defer s.cleanup()
	s.backendManager.Register(namespace, mockBackendClient)

	d := image.DigestFixture()

	mockBackendClient.EXPECT().Download(d.Hex(), gomock.Any()).Return(backend.ErrBlobNotFound)

	mi, err := cp.Provide(master1).GetMetaInfo(namespace, d)
	require.True(httputil.IsAccepted(err))
	require.Nil(mi)

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := cp.Provide(master1).GetMetaInfo(namespace, d)
		return !httputil.IsAccepted(err)
	}))

	mi, err = cp.Provide(master1).GetMetaInfo(namespace, d)
	require.True(httputil.IsNotFound(err))
	require.Nil(mi)
}

func TestUploadBlobReplicatesBlob(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider()

	for _, master := range []string{master1, master2} {
		s := newTestServer(master, config, cp)
		defer s.cleanup()
	}

	d, blob := computeBlobForHosts(config, master1, master2)

	err := cp.Provide(master1).UploadBlob(
		"test-namespace", d, bytes.NewReader(blob), false)
	require.NoError(err)

	for _, master := range []string{master1, master2} {
		ensureHasBlob(t, cp.Provide(master), d, blob)
	}
}

func TestUploadBlobResilientToReplicationFailure(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider()

	s := newTestServer(master1, config, cp)
	defer s.cleanup()

	cp.register(master2, "some broken address")

	// Master2 owns this blob, but it is not running. Uploads should still succeed
	// despite this.
	d, blob := computeBlobForHosts(config, master1, master2)

	err := cp.Provide(master1).UploadBlob(
		"test-namespace", d, bytes.NewReader(blob), false)
	require.NoError(err)

	ensureHasBlob(t, cp.Provide(master1), d, blob)
}

func TestUploadBlobThroughUploadsToStorageBackendAndReplicates(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := configFixture()
	cp := newTestClientProvider()
	namespace := "test-namespace"
	mockBackendClient := mockbackend.NewMockClient(ctrl)

	for _, master := range []string{master1, master2} {
		s := newTestServer(master, config, cp)
		defer s.cleanup()
		s.backendManager.Register(namespace, mockBackendClient)
	}

	d, blob := computeBlobForHosts(config, master1, master2)

	mockBackendClient.EXPECT().Upload(d.Hex(), fileio.MatchReader(blob)).Return(nil)

	err := cp.Provide(master1).UploadBlob(namespace, d, bytes.NewReader(blob), true)
	require.NoError(err)

	for _, master := range []string{master1, master2} {
		ensureHasBlob(t, cp.Provide(master), d, blob)
	}
}

func TestUploadBlobThroughCachedBlobStillUploadedToStorageBackend(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := configFixture()
	cp := newTestClientProvider()
	namespace := "test-namespace"
	mockBackendClient := mockbackend.NewMockClient(ctrl)

	for _, master := range []string{master1, master2} {
		s := newTestServer(master, config, cp)
		defer s.cleanup()
		s.backendManager.Register(namespace, mockBackendClient)
	}

	d, blob := computeBlobForHosts(config, master1, master2)

	mockBackendClient.EXPECT().Upload(d.Hex(), fileio.MatchReader(blob)).Return(nil).Times(2)

	err := cp.Provide(master1).UploadBlob(namespace, d, bytes.NewReader(blob), true)
	require.NoError(err)

	// Since we don't return error on backend upload, a second upload-through
	// operation should re-upload the blob to storage backend.
	err = cp.Provide(master1).UploadBlob(namespace, d, bytes.NewReader(blob), true)
	require.NoError(err)
}

func TestUploadBlobThroughDoesNotCommitBlobIfBackendUploadFails(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cp := newTestClientProvider()
	namespace := "test-namespace"
	mockBackendClient := mockbackend.NewMockClient(ctrl)

	s := newTestServer(master1, configNoRedirectFixture(), cp)
	defer s.cleanup()
	s.backendManager.Register(namespace, mockBackendClient)

	d, blob := image.DigestWithBlobFixture()

	mockBackendClient.EXPECT().Upload(d.Hex(), fileio.MatchReader(blob)).Return(errors.New("some error"))

	err := cp.Provide(master1).UploadBlob(namespace, d, bytes.NewReader(blob), true)
	require.Error(err)

	ok, err := cp.Provide(master1).CheckBlob(d)
	require.NoError(err)
	require.False(ok)
}

func TestPushBlob(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(master1, configNoRedirectFixture(), cp)
	defer s.cleanup()

	d, blob := image.DigestWithBlobFixture()

	err := cp.Provide(master1).PushBlob(d, bytes.NewReader(blob))
	require.NoError(err)
	ensureHasBlob(t, cp.Provide(master1), d, blob)

	// Ensure metainfo was generated.
	_, err = s.fs.States().Cache().GetMetadata(d.Hex(), store.NewTorrentMeta())
	require.NoError(err)

	// Pushing again should be a no-op.
	err = cp.Provide(master1).PushBlob(d, bytes.NewReader(blob))
	require.NoError(err)
	ensureHasBlob(t, cp.Provide(master1), d, blob)
}
