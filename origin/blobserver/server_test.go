// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package blobserver

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/persistedretry"
	"github.com/uber/kraken/lib/persistedretry/writeback"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/mockutil"
	"github.com/uber/kraken/utils/testutil"
)

func TestHealth(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	resp, err := httputil.Get(
		fmt.Sprintf("http://%s/health", s.addr))
	defer resp.Body.Close()
	require.NoError(err)
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(err)
	require.Equal("OK\n", string(b))
}

func TestStatHandlerLocalNotFound(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	d := core.DigestFixture()
	namespace := core.TagFixture()

	_, err := cp.Provide(s.host).StatLocal(namespace, d)
	require.Equal(blobclient.ErrBlobNotFound, err)
}

func TestStatHandlerInvalidParam(t *testing.T) {
	digest := core.DigestFixture()

	tests := []struct {
		desc   string
		path   string
		status int
	}{
		{
			"empty namespace",
			fmt.Sprintf("internal/namespace//blobs/%s", digest),
			http.StatusBadRequest,
		}, {
			"invalid digest",
			"internal/namespace/foo/blobs/bar",
			http.StatusBadRequest,
		}, {
			"invalid local param",
			fmt.Sprintf("internal/namespace/foo/blobs/%s?local=bar", digest),
			http.StatusInternalServerError,
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			cp := newTestClientProvider()

			s := newTestServer(t, master1, hashRingMaxReplica(), cp)
			defer s.cleanup()

			_, err := httputil.Head(fmt.Sprintf("http://%s/%s", s.addr, test.path))
			require.Error(err)
			require.True(httputil.IsStatus(err, test.status))
		})
	}
}

func TestStatHandlerNotFound(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	d := core.DigestFixture()
	namespace := core.TagFixture()

	backendClient := s.backendClient(namespace)

	backendClient.EXPECT().Stat(namespace, d.Hex()).Return(nil, backenderrors.ErrBlobNotFound)

	_, err := cp.Provide(master1).Stat(namespace, d)
	require.Equal(blobclient.ErrBlobNotFound, err)
}

func TestStatHandlerReturnSize(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
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

func TestDownloadBlobInvalidParam(t *testing.T) {
	digest := core.DigestFixture()

	tests := []struct {
		desc   string
		path   string
		status int
	}{
		{
			"empty namespace",
			fmt.Sprintf("namespace//blobs/%s", digest),
			http.StatusBadRequest,
		}, {
			"invalid digest",
			"namespace/foo/blobs/bar",
			http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			cp := newTestClientProvider()

			s := newTestServer(t, master1, hashRingMaxReplica(), cp)
			defer s.cleanup()

			_, err := httputil.Get(fmt.Sprintf("http://%s/%s", s.addr, test.path))
			require.Error(err)
			require.True(httputil.IsStatus(err, test.status))
		})
	}
}

func TestDownloadBlobNotFound(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	d := core.DigestFixture()
	namespace := core.TagFixture()

	backendClient := s.backendClient(namespace)
	backendClient.EXPECT().Stat(namespace, d.Hex()).Return(nil, backenderrors.ErrBlobNotFound)

	err := cp.Provide(master1).DownloadBlob(namespace, d, ioutil.Discard)
	require.Error(err)
	require.Equal(http.StatusNotFound, err.(httputil.StatusError).Status)
}

func TestDeleteBlob(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
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

func TestDeleteBlobInvalidParam(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	_, err := httputil.Delete(fmt.Sprintf("http://%s/internal/blobs/foo", s.addr))
	require.Error(err)
	require.True(httputil.IsStatus(err, http.StatusBadRequest))
}

func TestGetLocationsOK(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()
	ring := hashRingSomeReplica()

	s := newTestServer(t, master1, ring, cp)
	defer s.cleanup()

	blob := computeBlobForHosts(ring, master1, master2)

	locs, err := cp.Provide(s.host).Locations(blob.Digest)
	require.NoError(err)
	require.ElementsMatch([]string{master1, master2}, locs)
}

func TestGetPeerContextOK(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingSomeReplica(), cp)
	defer s.cleanup()

	pctx, err := cp.Provide(master1).GetPeerContext()
	require.NoError(err)
	require.Equal(s.pctx, pctx)
}

func TestGetMetaInfoDownloadsBlobAndReplicates(t *testing.T) {
	require := require.New(t)

	ring := hashRingSomeReplica()
	cp := newTestClientProvider()
	namespace := core.TagFixture()

	s1 := newTestServer(t, master1, ring, cp)
	defer s1.cleanup()

	s2 := newTestServer(t, master2, ring, cp)
	defer s2.cleanup()

	blob := computeBlobForHosts(ring, s1.host, s2.host)

	backendClient := s1.backendClient(namespace)
	backendClient.EXPECT().Stat(namespace,
		blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil).AnyTimes()
	backendClient.EXPECT().Download(namespace, blob.Digest.Hex(), mockutil.MatchWriter(blob.Content)).Return(nil)

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
	require.Equal(len(blob.Content), int(mi.Length()))

	// Ensure blob was replicated to other master.
	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := cp.Provide(master2).StatLocal(namespace, blob.Digest)
		return err == nil
	}))
}

func TestGetMetaInfoBlobNotFound(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	d := core.DigestFixture()
	namespace := core.TagFixture()

	backendClient := s.backendClient(namespace)
	backendClient.EXPECT().Stat(namespace, d.Hex()).Return(nil, backenderrors.ErrBlobNotFound)

	mi, err := cp.Provide(master1).GetMetaInfo(namespace, d)
	require.True(httputil.IsNotFound(err))
	require.Nil(mi)
}

func TestGetMetaInfoInvalidParam(t *testing.T) {
	digest := core.DigestFixture()

	tests := []struct {
		desc   string
		path   string
		status int
	}{
		{
			"empty namespace",
			fmt.Sprintf("internal/namespace//blobs/%s/metainfo", digest),
			http.StatusBadRequest,
		}, {
			"invalid digest",
			"internal/namespace/foo/blobs/bar/metainfo",
			http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			cp := newTestClientProvider()

			s := newTestServer(t, master1, hashRingMaxReplica(), cp)
			defer s.cleanup()

			_, err := httputil.Get(fmt.Sprintf("http://%s/%s", s.addr, test.path))
			require.Error(err)
			require.True(httputil.IsStatus(err, test.status))
		})
	}
}

func TestTransferBlob(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
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

func TestTransferBlobInvalidParam(t *testing.T) {
	t.Run("StartInvalidDigest", func(t *testing.T) {
		require := require.New(t)

		cp := newTestClientProvider()
		s := newTestServer(t, master1, hashRingMaxReplica(), cp)
		defer s.cleanup()

		_, err := httputil.Post(
			fmt.Sprintf("http://%s/internal/blobs/foo/uploads", s.addr))
		require.Error(err)
		require.True(httputil.IsStatus(err, http.StatusBadRequest))
	})
	t.Run("PatchInvalidDigest", func(t *testing.T) {
		require := require.New(t)

		cp := newTestClientProvider()
		s := newTestServer(t, master1, hashRingMaxReplica(), cp)
		defer s.cleanup()

		d := core.DigestFixture()
		_, err := httputil.Post(
			fmt.Sprintf("http://%s/internal/blobs/%s/uploads", s.addr, d.String()))
		require.NoError(err)
		_, err = httputil.Patch(
			fmt.Sprintf("http://%s/internal/blobs/foo/uploads/bar", s.addr),
			httputil.SendHeaders(map[string]string{
				"Content-Range": fmt.Sprintf("%d-%d", 0, 0),
			}))
		require.Error(err)
		require.True(httputil.IsStatus(err, http.StatusBadRequest))
	})
	t.Run("PatchNonexistentUploadUUID", func(t *testing.T) {
		require := require.New(t)

		cp := newTestClientProvider()
		s := newTestServer(t, master1, hashRingMaxReplica(), cp)
		defer s.cleanup()

		d := core.DigestFixture()
		_, err := httputil.Post(
			fmt.Sprintf("http://%s/internal/blobs/%s/uploads", s.addr, d.String()))
		require.NoError(err)

		_, err = httputil.Patch(
			fmt.Sprintf("http://%s/internal/blobs/%s/uploads/bar", s.addr, d.String()),
			httputil.SendHeaders(map[string]string{
				"Content-Range": fmt.Sprintf("%d-%d", 0, 0),
			}))
		require.Error(err)
		require.True(httputil.IsStatus(err, http.StatusNotFound))
	})
	t.Run("CommitInvalidDigest", func(t *testing.T) {
		require := require.New(t)

		cp := newTestClientProvider()
		s := newTestServer(t, master1, hashRingMaxReplica(), cp)
		defer s.cleanup()

		d := core.DigestFixture()
		_, err := httputil.Post(
			fmt.Sprintf("http://%s/internal/blobs/%s/uploads", s.addr, d.String()))
		require.NoError(err)

		_, err = httputil.Put(
			fmt.Sprintf("http://%s/internal/blobs/foo/uploads/bar", s.addr))
		require.Error(err)
		require.True(httputil.IsStatus(err, http.StatusBadRequest))
	})
	t.Run("CommitNonexistentUploadUUID", func(t *testing.T) {
		require := require.New(t)

		cp := newTestClientProvider()
		s := newTestServer(t, master1, hashRingMaxReplica(), cp)
		defer s.cleanup()

		d := core.DigestFixture()
		_, err := httputil.Post(
			fmt.Sprintf("http://%s/internal/blobs/%s/uploads", s.addr, d.String()))
		require.NoError(err)

		_, err = httputil.Put(
			fmt.Sprintf("http://%s/internal/blobs/%s/uploads/bar", s.addr, d.String()))
		require.Error(err)
		fmt.Println(err)
		require.True(httputil.IsStatus(err, http.StatusNotFound))
	})
}

func TestTransferBlobSmallChunkSize(t *testing.T) {
	require := require.New(t)

	s := newTestServer(t, master1, hashRingMaxReplica(), newTestClientProvider())
	defer s.cleanup()

	blob := core.SizedBlobFixture(1000, 1)
	namespace := core.TagFixture()

	client := blobclient.New(s.addr, blobclient.WithChunkSize(13))

	err := client.TransferBlob(blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)
	ensureHasBlob(t, client, namespace, blob)
}

func TestOverwriteMetainfo(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	err := cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)

	mi, err := cp.Provide(master1).GetMetaInfo(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(int64(4), mi.PieceLength())

	err = cp.Provide(master1).OverwriteMetaInfo(blob.Digest, 16)
	require.NoError(err)

	mi, err = cp.Provide(master1).GetMetaInfo(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(int64(16), mi.PieceLength())
}

func TestReplicateToRemote(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	require.NoError(cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content)))

	remote := "remote:80"

	remoteCluster := s.expectRemoteCluster(remote)
	remoteCluster.EXPECT().UploadBlob(
		namespace, blob.Digest, mockutil.MatchReader(blob.Content)).Return(nil)

	require.NoError(cp.Provide(master1).ReplicateToRemote(namespace, blob.Digest, remote))
}

func TestReplicateToRemoteInvalidParam(t *testing.T) {
	digest := core.DigestFixture()

	tests := []struct {
		desc   string
		path   string
		status int
	}{
		{
			"empty namespace",
			fmt.Sprintf("namespace//blobs/%s/remote/bar", digest),
			http.StatusBadRequest,
		}, {
			"invalid digest",
			"namespace/hello/blobs/foo/remote/bar",
			http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			cp := newTestClientProvider()

			s := newTestServer(t, master1, hashRingMaxReplica(), cp)
			defer s.cleanup()

			_, err := httputil.Post(fmt.Sprintf("http://%s/%s", s.addr, test.path))
			require.Error(err)
			require.True(httputil.IsStatus(err, test.status))
		})
	}
}

func TestReplicateToRemoteWhenBlobInStorageBackend(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	backendClient := s.backendClient(namespace)
	backendClient.EXPECT().Stat(namespace,
		blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil).AnyTimes()
	backendClient.EXPECT().Download(namespace, blob.Digest.Hex(), mockutil.MatchWriter(blob.Content)).Return(nil)

	remote := "remote:80"

	remoteCluster := s.expectRemoteCluster(remote)
	remoteCluster.EXPECT().UploadBlob(
		namespace, blob.Digest, mockutil.MatchReader(blob.Content)).Return(nil)

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		err := cp.Provide(master1).ReplicateToRemote(namespace, blob.Digest, remote)
		return !httputil.IsAccepted(err)
	}))
}

func TestUploadBlobDuplicatesWriteBackTaskToReplicas(t *testing.T) {
	require := require.New(t)

	ring := hashRingSomeReplica()
	namespace := core.TagFixture()

	cp := newTestClientProvider()

	s1 := newTestServer(t, master1, ring, cp)
	defer s1.cleanup()

	s2 := newTestServer(t, master2, ring, cp)
	defer s2.cleanup()

	blob := computeBlobForHosts(ring, s1.host, s2.host)

	s1.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(namespace, blob.Digest.Hex(), 0))).Return(nil)
	s2.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(namespace, blob.Digest.Hex(), 30*time.Minute)))

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

	ring := hashRingNoReplica()
	namespace := core.TagFixture()

	cp := newTestClientProvider()

	s := newTestServer(t, master1, ring, cp)
	defer s.cleanup()

	blob := computeBlobForHosts(ring, s.host)

	expectedTask := writeback.MatchTask(writeback.NewTask(namespace, blob.Digest.Hex(), 0))

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

	ring := hashRingSomeReplica()
	namespace := core.TagFixture()

	cp := newTestClientProvider()

	s := newTestServer(t, master1, ring, cp)
	defer s.cleanup()

	cp.register(master2, blobclient.New("localhost:0"))

	blob := computeBlobForHosts(ring, s.host, master2)

	s.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(namespace, blob.Digest.Hex(), 0))).Return(nil)

	err := cp.Provide(s.host).UploadBlob(namespace, blob.Digest, bytes.NewReader(blob.Content))
	require.NoError(err)

	ensureHasBlob(t, cp.Provide(s.host), namespace, blob)
}

func TestForceCleanupTTL(t *testing.T) {
	require := require.New(t)

	ring := hashRingNoReplica()
	namespace := core.TagFixture()

	cp := newTestClientProvider()

	s := newTestServer(t, master1, ring, cp)
	defer s.cleanup()

	client := cp.Provide(s.host)

	blob := computeBlobForHosts(ring, s.host)

	s.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(namespace, blob.Digest.Hex(), 0))).Return(nil)

	require.NoError(client.UploadBlob(namespace, blob.Digest, bytes.NewReader(blob.Content)))

	ensureHasBlob(t, client, namespace, blob)

	// Since the blob was just uploaded, it should not be deleted on force cleanup.
	require.NoError(client.ForceCleanup(12 * time.Hour))
	ensureHasBlob(t, client, namespace, blob)

	s.clk.Add(14 * time.Hour)

	s.writeBackManager.EXPECT().Find(writeback.NewNameQuery(blob.Digest.Hex())).Return(nil, nil)

	require.NoError(client.ForceCleanup(12 * time.Hour))

	_, err := client.StatLocal(namespace, blob.Digest)
	require.Error(err)
	require.Equal(blobclient.ErrBlobNotFound, err)
}

func TestForceCleanupNonOwner(t *testing.T) {
	require := require.New(t)

	ring := hashRingNoReplica()
	namespace := core.TagFixture()

	cp := newTestClientProvider()

	s1 := newTestServer(t, master1, ring, cp)
	defer s1.cleanup()

	s2 := newTestServer(t, master2, ring, cp)
	defer s2.cleanup()

	client := cp.Provide(s1.host)

	// s1 does not own blob, but will still accept the upload. On ForceCleanup, it
	// should be removed.
	blob := computeBlobForHosts(ring, s2.host)

	s1.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(namespace, blob.Digest.Hex(), 0))).Return(nil)

	s2.writeBackManager.EXPECT().Add(
		writeback.MatchTask(writeback.NewTask(namespace, blob.Digest.Hex(), 30*time.Minute)))

	require.NoError(client.UploadBlob(namespace, blob.Digest, bytes.NewReader(blob.Content)))

	ensureHasBlob(t, client, namespace, blob)

	s1.writeBackManager.EXPECT().Find(writeback.NewNameQuery(blob.Digest.Hex())).Return(nil, nil)

	require.NoError(client.ForceCleanup(12 * time.Hour))

	_, err := client.StatLocal(namespace, blob.Digest)
	require.Error(err)
	require.Equal(blobclient.ErrBlobNotFound, err)
}

func TestForceCleanupWriteBackFailures(t *testing.T) {
	require := require.New(t)

	ring := hashRingNoReplica()
	namespace := core.TagFixture()

	cp := newTestClientProvider()

	s := newTestServer(t, master1, ring, cp)
	defer s.cleanup()

	client := cp.Provide(s.host)

	blob := computeBlobForHosts(ring, s.host)

	task := writeback.NewTask(namespace, blob.Digest.Hex(), 0)

	s.writeBackManager.EXPECT().Add(writeback.MatchTask(task)).Return(nil)

	require.NoError(client.UploadBlob(namespace, blob.Digest, bytes.NewReader(blob.Content)))

	ensureHasBlob(t, client, namespace, blob)

	s.clk.Add(14 * time.Hour)

	// If there exists a writeback task, and it fails to manually execute it,
	// the blob should not be deleted.
	s.writeBackManager.EXPECT().Find(
		writeback.NewNameQuery(blob.Digest.Hex())).Return([]persistedretry.Task{task}, nil)

	s.writeBackManager.EXPECT().SyncExec(task).Return(errors.New("some error"))

	require.NoError(client.ForceCleanup(12 * time.Hour))

	ensureHasBlob(t, client, namespace, blob)
}
