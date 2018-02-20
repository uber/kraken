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

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"
)

func TestCheckBlobHandlerOK(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configMaxReplicaFixture())
	defer stop()

	d := core.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, nil)

	ok, err := blobclient.New(addr).CheckBlob(d)
	require.NoError(err)
	require.True(ok)
}

func TestCheckBlobHandlerNotFound(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configMaxReplicaFixture())
	defer stop()

	d := core.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, os.ErrNotExist)

	ok, err := blobclient.New(addr).CheckBlob(d)
	require.NoError(err)
	require.False(ok)
}

func TestGetBlobHandlerOK(t *testing.T) {
	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configMaxReplicaFixture())
	defer stop()

	d := core.DigestFixture()

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

	addr, stop := mocks.server(configMaxReplicaFixture())
	defer stop()

	d := core.DigestFixture()

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

	d := core.DigestFixture()

	mocks.fileStore.EXPECT().DeleteCacheFile(d.Hex()).Return(nil)

	require.NoError(blobclient.New(addr).DeleteBlob(d))
}

func TestDeleteBlobHandlerNotFound(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configFixture())
	defer stop()

	d := core.DigestFixture()

	mocks.fileStore.EXPECT().DeleteCacheFile(d.Hex()).Return(os.ErrNotExist)

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

func TestIncorrectNodeErrors(t *testing.T) {
	config := configFixture()
	d, _ := computeBlobForHosts(config, master2, master3)

	tests := []struct {
		name string
		f    func(c blobclient.Client) error
	}{
		{
			"CheckBlob",
			func(c blobclient.Client) error { _, err := c.CheckBlob(d); return err },
		}, {
			"GetBlob",
			func(c blobclient.Client) error { _, err := c.GetBlob(d); return err },
		}, {
			"TransferBlob",
			func(c blobclient.Client) error { return c.TransferBlob(d, bytes.NewBufferString("blah"), 4) },
		}, {
			"GetMetaInfo",
			func(c blobclient.Client) error { _, err := c.GetMetaInfo(namespace, d); return err },
		}, {
			"OverwriteMetaInfo",
			func(c blobclient.Client) error { return c.OverwriteMetaInfo(d, 64) },
		}, {
			"UploadBlob",
			func(c blobclient.Client) error {
				return c.UploadBlob(namespace, d, bytes.NewBufferString("blah"), 4, false)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			mocks := newServerMocks(t)
			defer mocks.ctrl.Finish()

			addr, stop := mocks.server(config)
			defer stop()

			err := test.f(blobclient.New(addr))
			require.Error(err)
			require.True(httputil.IsStatus(err, http.StatusBadRequest))
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
	mockBackendClient := mockbackend.NewMockClient(ctrl)

	for _, master := range []string{master1, master2} {
		s := newTestServer(master, configFixture(), cp)
		defer s.cleanup()
		s.backendManager.Register(namespace, mockBackendClient)
	}

	d, blob := computeBlobForHosts(config, master1, master2)

	mockBackendClient.EXPECT().Download(d.Hex(), rwutil.MatchWriter(blob)).Return(nil)

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
	mockBackendClient := mockbackend.NewMockClient(ctrl)

	cp := newTestClientProvider()

	s := newTestServer(master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()
	s.backendManager.Register(namespace, mockBackendClient)

	d := core.DigestFixture()

	mockBackendClient.EXPECT().Download(d.Hex(), gomock.Any()).Return(backenderrors.ErrBlobNotFound)

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
		namespace, d, bytes.NewReader(blob), int64(len(blob)), false)
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
		namespace, d, bytes.NewReader(blob), int64(len(blob)), false)
	require.NoError(err)

	ensureHasBlob(t, cp.Provide(master1), d, blob)
}

func TestUploadBlobThroughUploadsToStorageBackendAndReplicates(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := configFixture()
	cp := newTestClientProvider()
	mockBackendClient := mockbackend.NewMockClient(ctrl)

	for _, master := range []string{master1, master2} {
		s := newTestServer(master, config, cp)
		defer s.cleanup()
		s.backendManager.Register(namespace, mockBackendClient)
	}

	d, blob := computeBlobForHosts(config, master1, master2)

	mockBackendClient.EXPECT().Upload(d.Hex(), rwutil.MatchReader(blob)).Return(nil)

	err := cp.Provide(master1).UploadBlob(
		namespace, d, bytes.NewReader(blob), int64(len(blob)), true)
	require.NoError(err)

	for _, master := range []string{master1, master2} {
		ensureHasBlob(t, cp.Provide(master), d, blob)
	}
}

func TestUploadBlobThroughDoesNotCommitBlobIfBackendUploadFails(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cp := newTestClientProvider()
	mockBackendClient := mockbackend.NewMockClient(ctrl)

	s := newTestServer(master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()
	s.backendManager.Register(namespace, mockBackendClient)

	d, blob := core.DigestWithBlobFixture()

	mockBackendClient.EXPECT().Upload(d.Hex(), rwutil.MatchReader(blob)).Return(errors.New("some error"))

	err := cp.Provide(master1).UploadBlob(
		namespace, d, bytes.NewReader(blob), int64(len(blob)), true)
	require.Error(err)

	ok, err := cp.Provide(master1).CheckBlob(d)
	require.NoError(err)
	require.False(ok)
}

func TestTransferBlob(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	d, blob := core.DigestWithBlobFixture()

	err := cp.Provide(master1).TransferBlob(d, bytes.NewReader(blob), int64(len(blob)))
	require.NoError(err)
	ensureHasBlob(t, cp.Provide(master1), d, blob)

	// Ensure metainfo was generated.
	_, err = s.fs.GetCacheFileMetadata(d.Hex(), store.NewTorrentMeta())
	require.NoError(err)

	// Pushing again should be a no-op.
	err = cp.Provide(master1).TransferBlob(d, bytes.NewReader(blob), int64(len(blob)))
	require.NoError(err)
	ensureHasBlob(t, cp.Provide(master1), d, blob)
}

func TestOverwriteMetainfo(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	d, blob := core.DigestWithBlobFixture()

	err := cp.Provide(master1).TransferBlob(d, bytes.NewReader(blob), int64(len(blob)))
	require.NoError(err)

	mi, err := cp.Provide(master1).GetMetaInfo(namespace, d)
	require.NoError(err)
	require.Equal(int64(4), mi.Info.PieceLength)

	err = cp.Provide(master1).OverwriteMetaInfo(d, 16)
	require.NoError(err)

	mi, err = cp.Provide(master1).GetMetaInfo(namespace, d)
	require.NoError(err)
	require.Equal(int64(16), mi.Info.PieceLength)
}
