package blobserver

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/persistedretry/writeback"
	"code.uber.internal/infra/kraken/lib/store/metadata"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"
)

func TestStatHandlerLocalNotFound(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	d := core.DigestFixture()
	namespace := core.TagFixture()

	_, err := cp.Provide(s.host).StatLocal(namespace, d)
	require.Equal(blobclient.ErrBlobNotFound, err)
}

func TestStatHandlerNotFound(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	d := core.DigestFixture()
	namespace := core.TagFixture()

	backendClient := s.backendClient(namespace)

	backendClient.EXPECT().Stat(d.Hex()).Return(nil, backenderrors.ErrBlobNotFound)

	_, err := cp.Provide(master1).Stat(namespace, d)
	require.Equal(blobclient.ErrBlobNotFound, err)
}

func TestStatHandlerReturnSize(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	client := cp.Provide(s.host)
	blob := core.SizedBlobFixture(256, 8)
	namespace := core.TagFixture()

	require.NoError(client.TransferBlob(blob.Digest, bytes.NewReader(blob.Content)))

	ensureHasBlob(t, cp.Provide(s.host), namespace, blob)

	bi, err := cp.Provide(master1).Stat(namespace, blob.Digest)
	require.NoError(err)
	require.NotNil(bi)
	require.Equal(int64(256), bi.Size)
}

func TestDownloadBlobHandlerNotFound(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	d := core.DigestFixture()
	namespace := core.TagFixture()

	backendClient := s.backendClient(namespace)
	backendClient.EXPECT().Stat(d.Hex()).Return(nil, backenderrors.ErrBlobNotFound)

	err := cp.Provide(master1).DownloadBlob(namespace, d, ioutil.Discard)
	require.Error(err)
	require.Equal(http.StatusNotFound, err.(httputil.StatusError).Status)
}

func TestDeleteBlob(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	client := cp.Provide(s.host)

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	require.NoError(client.TransferBlob(blob.Digest, bytes.NewReader(blob.Content)))

	ensureHasBlob(t, cp.Provide(s.host), namespace, blob)

	require.NoError(client.DeleteBlob(blob.Digest))

	_, err := client.StatLocal(namespace, blob.Digest)
	require.Equal(blobclient.ErrBlobNotFound, err)
}

func TestGetLocationsHandlerOK(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()
	config := configFixture()

	s := newTestServer(t, master1, config, cp)
	defer s.cleanup()

	blob := computeBlobForHosts(config, master1, master2)

	locs, err := cp.Provide(s.host).Locations(blob.Digest)
	require.NoError(err)
	require.Equal([]string{master1, master2}, locs)
}

func TestIncorrectNodeErrors(t *testing.T) {
	config := configFixture()
	namespace := core.TagFixture()
	blob := computeBlobForHosts(config, master2, master3)

	tests := []struct {
		name string
		f    func(c blobclient.Client) error
	}{
		{
			"Stat",
			func(c blobclient.Client) error { _, err := c.Stat(namespace, blob.Digest); return err },
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

			cp := newTestClientProvider()

			s := newTestServer(t, master1, config, cp)
			defer s.cleanup()

			err := test.f(cp.Provide(s.host))
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
	namespace := core.TagFixture()

	s1 := newTestServer(t, master1, config, cp)
	defer s1.cleanup()

	s2 := newTestServer(t, master2, config, cp)
	defer s2.cleanup()

	blob := computeBlobForHosts(config, s1.host, s2.host)

	backendClient := s1.backendClient(namespace)
	backendClient.EXPECT().Stat(
		blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil).AnyTimes()
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
		_, err := cp.Provide(master2).StatLocal(namespace, blob.Digest)
		return err == nil
	}))
}

func TestGetMetaInfoHandlerBlobNotFound(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	d := core.DigestFixture()
	namespace := core.TagFixture()

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
	namespace := core.TagFixture()

	err := cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)
	ensureHasBlob(t, cp.Provide(master1), namespace, blob)

	// Ensure metainfo was generated.
	var tm metadata.TorrentMeta
	require.NoError(s.cas.GetCacheFileMetadata(blob.Digest.Hex(), &tm))

	// Pushing again should be a no-op.
	err = cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)
	ensureHasBlob(t, cp.Provide(master1), namespace, blob)
}

func TestTransferBlobSmallChunkSize(t *testing.T) {
	require := require.New(t)

	s := newTestServer(t, master1, configMaxReplicaFixture(), newTestClientProvider())
	defer s.cleanup()

	blob := core.SizedBlobFixture(1000, 1)
	namespace := core.TagFixture()

	client := blobclient.NewWithConfig(s.addr, blobclient.Config{ChunkSize: 13})

	err := client.TransferBlob(blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)
	ensureHasBlob(t, client, namespace, blob)
}

func TestOverwriteMetainfo(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, configMaxReplicaFixture(), cp)
	defer s.cleanup()

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

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
	namespace := core.TagFixture()

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
	namespace := core.TagFixture()

	backendClient := s.backendClient(namespace)
	backendClient.EXPECT().Stat(
		blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil).AnyTimes()
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
	namespace := core.TagFixture()

	cp := newTestClientProvider()

	s1 := newTestServer(t, master1, config, cp)
	defer s1.cleanup()

	s2 := newTestServer(t, master2, config, cp)
	defer s2.cleanup()

	blob := computeBlobForHosts(config, s1.host, s2.host)

	s1.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(namespace, blob.Digest.Hex()))).Return(nil)
	s2.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTaskWithDelay(namespace, blob.Digest.Hex(), time.Minute)))

	err := cp.Provide(s1.host).UploadBlob(namespace, blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)

	ensureHasBlob(t, cp.Provide(s1.host), namespace, blob)
	ensureHasBlob(t, cp.Provide(s2.host), namespace, blob)

	// Shouldn't be able to delete blob since it is still being written back.
	require.Error(cp.Provide(s1.host).DeleteBlob(blob.Digest))
	require.Error(cp.Provide(s2.host).DeleteBlob(blob.Digest))
}

func TestUploadBlobRetriesWriteBackFailure(t *testing.T) {
	require := require.New(t)

	config := configNoReplicaFixture()
	namespace := core.TagFixture()

	cp := newTestClientProvider()

	s := newTestServer(t, master1, config, cp)
	defer s.cleanup()

	blob := computeBlobForHosts(config, s.host)

	expectedTask := writeback.MatchTask(writeback.NewTask(namespace, blob.Digest.Hex()))

	gomock.InOrder(
		s.writeBackManager.EXPECT().Add(expectedTask).Return(errors.New("some error")),
		s.writeBackManager.EXPECT().Add(expectedTask).Return(nil),
	)

	// Upload should "fail" because we failed to add a write-back task, but blob
	// should still be present.
	err := cp.Provide(s.host).UploadBlob(namespace, blob.Digest, bytes.NewReader(blob.Content))
	require.Error(err)
	ensureHasBlob(t, cp.Provide(s.host), namespace, blob)

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
	namespace := core.TagFixture()

	cp := newTestClientProvider()

	s := newTestServer(t, master1, config, cp)
	defer s.cleanup()

	cp.register(master2, blobclient.New("dummy-addr"))

	blob := computeBlobForHosts(config, s.host, master2)

	s.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(namespace, blob.Digest.Hex()))).Return(nil)

	err := cp.Provide(s.host).UploadBlob(namespace, blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)

	ensureHasBlob(t, cp.Provide(s.host), namespace, blob)
}
