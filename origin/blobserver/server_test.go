package blobserver

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/backend/blobinfo"
	"code.uber.internal/infra/kraken/lib/persistedretry/writeback"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/store/metadata"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"
)

func TestCheckBlobHandlerLocalOK(t *testing.T) {
	t.Skip("TODO: fails on jenkins")

	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configFixture())
	defer stop()

	d := core.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, nil)

	ok, err := blobclient.New(addr).CheckLocalBlob(namespace, d)
	require.NoError(err)
	require.True(ok)
}

func TestCheckBlobHandlerOK(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	d := core.DigestFixture()

	backendClient := s.backendClient(namespace)

	backendClient.EXPECT().Stat(d.Hex()).Return(blobinfo.New(int64(len(d.Hex()))), nil)

	ok, err := cp.Provide(master1).CheckBlob(namespace, d)
	require.NoError(err)
	require.True(ok)
}

func TestCheckBlobHandlerLocalNotFound(t *testing.T) {
	require := require.New(t)

	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configMaxReplicaFixture())
	defer stop()

	d := core.DigestFixture()

	mocks.fileStore.EXPECT().GetCacheFileStat(d.Hex()).Return(nil, os.ErrNotExist)

	ok, err := blobclient.New(addr).CheckLocalBlob(namespace, d)
	require.NoError(err)
	require.False(ok)
}

func TestCheckBlobHandlerNotFound(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	d := core.DigestFixture()

	backendClient := s.backendClient(namespace)

	backendClient.EXPECT().Stat(d.Hex()).Return(nil, backenderrors.ErrBlobNotFound)

	ok, err := cp.Provide(master1).CheckBlob(namespace, d)
	require.NoError(err)
	require.False(ok)
}

func TestDownloadBlobHandlerOK(t *testing.T) {
	mocks := newServerMocks(t)
	defer mocks.ctrl.Finish()

	addr, stop := mocks.server(configMaxReplicaFixture())
	defer stop()

	blob := core.NewBlobFixture()

	f, cleanup := store.NewMockFileReadWriter(blob.Content)
	defer cleanup()

	mocks.fileStore.EXPECT().GetCacheFileReader(blob.Digest.Hex()).Return(f, nil)

	ensureHasBlob(t, blobclient.New(addr), blob)
}

func TestDownloadBlobHandlerNotFound(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	d := core.DigestFixture()

	backendClient := s.backendClient(namespace)
	backendClient.EXPECT().Stat(d.Hex()).Return(nil, backenderrors.ErrBlobNotFound)

	err := cp.Provide(master1).DownloadBlob(namespace, d, ioutil.Discard)
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

	blob := computeBlobForHosts(config, master1, master2)

	locs, err := blobclient.New(addr).Locations(blob.Digest)
	require.NoError(err)
	require.Equal([]string{master1, master2}, locs)
}

func TestIncorrectNodeErrors(t *testing.T) {
	config := configFixture()
	blob := computeBlobForHosts(config, master2, master3)

	tests := []struct {
		name string
		f    func(c blobclient.Client) error
	}{
		{
			"CheckBlob",
			func(c blobclient.Client) error { _, err := c.CheckBlob(namespace, blob.Digest); return err },
		}, {
			"DownloadBlob",
			func(c blobclient.Client) error {
				return c.DownloadBlob(namespace, blob.Digest, ioutil.Discard)
			},
		}, {
			"TransferBlob",
			func(c blobclient.Client) error {
				return c.TransferBlob(blob.Digest, bytes.NewBufferString("blah"))
			},
		}, {
			"GetMetaInfo",
			func(c blobclient.Client) error {
				_, err := c.GetMetaInfo(namespace, blob.Digest)
				return err
			},
		}, {
			"OverwriteMetaInfo",
			func(c blobclient.Client) error { return c.OverwriteMetaInfo(blob.Digest, 64) },
		}, {
			"UploadBlob",
			func(c blobclient.Client) error {
				return c.UploadBlob(namespace, blob.Digest, bytes.NewBufferString("blah"))
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

	s := newTestServer(t, master1, configFixture(), cp)
	defer s.cleanup()

	pctx, err := cp.Provide(master1).GetPeerContext()
	require.NoError(err)
	require.Equal(s.pctx, pctx)
}

func TestGetMetaInfoHandlerDownloadsBlobAndReplicates(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider()

	s1 := newTestServer(t, master1, config, cp)
	defer s1.cleanup()

	s2 := newTestServer(t, master2, config, cp)
	defer s2.cleanup()

	blob := computeBlobForHosts(config, s1.host, s2.host)

	backendClient := s1.backendClient(namespace)
	backendClient.EXPECT().Stat(
		blob.Digest.Hex()).Return(blobinfo.New(int64(len(blob.Content))), nil).AnyTimes()
	backendClient.EXPECT().Download(blob.Digest.Hex(), rwutil.MatchWriter(blob.Content)).Return(nil)

	mi, err := cp.Provide(master1).GetMetaInfo(namespace, blob.Digest)
	require.True(httputil.IsAccepted(err))
	require.Nil(mi)

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := cp.Provide(master1).GetMetaInfo(namespace, blob.Digest)
		return !httputil.IsAccepted(err)
	}))

	mi, err = cp.Provide(master1).GetMetaInfo(namespace, blob.Digest)
	require.NoError(err)
	require.NotNil(mi)
	require.Equal(len(blob.Content), int(mi.Info.Length))

	// Ensure blob was replicated to other master.
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		ok, err := cp.Provide(master2).CheckLocalBlob(namespace, blob.Digest)
		return ok && err == nil
	}))
}

func TestGetMetaInfoHandlerBlobNotFound(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	d := core.DigestFixture()

	backendClient := s.backendClient(namespace)
	backendClient.EXPECT().Stat(d.Hex()).Return(nil, backenderrors.ErrBlobNotFound)

	mi, err := cp.Provide(master1).GetMetaInfo(namespace, d)
	require.True(httputil.IsNotFound(err))
	require.Nil(mi)
}

func TestTransferBlob(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	blob := core.NewBlobFixture()

	err := cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)
	ensureHasBlob(t, cp.Provide(master1), blob)

	// Ensure metainfo was generated.
	var tm metadata.TorrentMeta
	require.NoError(s.fs.GetCacheFileMetadata(blob.Digest.Hex(), &tm))

	// Pushing again should be a no-op.
	err = cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)
	ensureHasBlob(t, cp.Provide(master1), blob)
}

func TestTransferBlobSmallChunkSize(t *testing.T) {
	require := require.New(t)

	s := newTestServer(t, master1, configMaxReplicaFixture(), newTestClientProvider())
	defer s.cleanup()

	blob := core.SizedBlobFixture(1000, 1)

	client := blobclient.NewWithConfig(s.addr, blobclient.Config{ChunkSize: 13})

	err := client.TransferBlob(blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)
	ensureHasBlob(t, client, blob)
}

func TestOverwriteMetainfo(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	blob := core.NewBlobFixture()

	err := cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)

	mi, err := cp.Provide(master1).GetMetaInfo(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(int64(4), mi.Info.PieceLength)

	err = cp.Provide(master1).OverwriteMetaInfo(blob.Digest, 16)
	require.NoError(err)

	mi, err = cp.Provide(master1).GetMetaInfo(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(int64(16), mi.Info.PieceLength)
}

func TestReplicateToRemote(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	blob := core.NewBlobFixture()

	require.NoError(cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content)))

	remote := "some-remote-origin"
	remoteClient := s.remoteClient(remote)
	remoteClient.EXPECT().Locations(blob.Digest).Return([]string{remote}, nil)
	remoteClient.EXPECT().UploadBlob(
		namespace, blob.Digest, rwutil.MatchReader(blob.Content)).Return(nil)

	require.NoError(cp.Provide(master1).ReplicateToRemote(namespace, blob.Digest, remote))
}

func TestReplicateToRemoteWhenBlobInStorageBackend(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	blob := core.NewBlobFixture()

	backendClient := s.backendClient(namespace)
	backendClient.EXPECT().Stat(
		blob.Digest.Hex()).Return(blobinfo.New(int64(len(blob.Content))), nil).AnyTimes()
	backendClient.EXPECT().Download(blob.Digest.Hex(), rwutil.MatchWriter(blob.Content)).Return(nil)

	remote := "some-remote-origin"
	remoteClient := s.remoteClient(remote)
	remoteClient.EXPECT().Locations(blob.Digest).Return([]string{remote}, nil)
	remoteClient.EXPECT().UploadBlob(
		namespace, blob.Digest, rwutil.MatchReader(blob.Content)).Return(nil)

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		err := cp.Provide(master1).ReplicateToRemote(namespace, blob.Digest, remote)
		return !httputil.IsAccepted(err)
	}))
}

func TestUploadBlobDuplicatesWriteBackTaskToReplicas(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	config.DuplicateWriteBackStagger = time.Minute

	cp := newTestClientProvider()

	s1 := newTestServer(t, master1, config, cp)
	defer s1.cleanup()

	s2 := newTestServer(t, master2, config, cp)
	defer s2.cleanup()

	blob := computeBlobForHosts(config, s1.host, s2.host)

	s1.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(namespace, blob.Digest))).Return(nil)
	s2.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTaskWithDelay(namespace, blob.Digest, time.Minute)))

	err := cp.Provide(s1.host).UploadBlob(namespace, blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)

	ensureHasBlob(t, cp.Provide(s1.host), blob)
	ensureHasBlob(t, cp.Provide(s2.host), blob)

	// Shouldn't be able to delete blob since it is still being written back.
	require.Error(cp.Provide(s1.host).DeleteBlob(blob.Digest))
	require.Error(cp.Provide(s2.host).DeleteBlob(blob.Digest))
}

func TestUploadBlobRetriesWriteBackFailure(t *testing.T) {
	require := require.New(t)

	config := configNoReplicaFixture()

	cp := newTestClientProvider()

	s := newTestServer(t, master1, config, cp)
	defer s.cleanup()

	blob := computeBlobForHosts(config, s.host)

	expectedTask := writeback.MatchTask(writeback.NewTask(namespace, blob.Digest))

	gomock.InOrder(
		s.writeBackManager.EXPECT().Add(expectedTask).Return(errors.New("some error")),
		s.writeBackManager.EXPECT().Add(expectedTask).Return(nil),
	)

	// Upload should "fail" because we failed to add a write-back task, but blob
	// should still be present.
	err := cp.Provide(s.host).UploadBlob(namespace, blob.Digest, bytes.NewReader(blob.Content))
	require.Error(err)
	ensureHasBlob(t, cp.Provide(s.host), blob)

	// Uploading again should succeed.
	err = cp.Provide(s.host).UploadBlob(namespace, blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)

	// Shouldn't be able to delete blob since it is still being written back.
	require.Error(cp.Provide(s.host).DeleteBlob(blob.Digest))
}

func TestUploadBlobResilientToDuplicationFailure(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	config.DuplicateWriteBackStagger = time.Minute

	cp := newTestClientProvider()

	s := newTestServer(t, master1, config, cp)
	defer s.cleanup()

	cp.register(master2, blobclient.New("dummy-addr"))

	blob := computeBlobForHosts(config, s.host, master2)

	s.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(namespace, blob.Digest))).Return(nil)

	err := cp.Provide(s.host).UploadBlob(namespace, blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)

	ensureHasBlob(t, cp.Provide(s.host), blob)
}
