// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package blobserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/persistedretry/writeback"
	"github.com/uber/kraken/lib/store/disk"
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
	require.NoError(err)
	t.Cleanup(func() {
		require.NoError(resp.Body.Close())
	})
	b, err := io.ReadAll(resp.Body)
	require.NoError(err)
	require.Equal("OK\n", string(b))
}

func TestReadiness(t *testing.T) {
	for _, tc := range []struct {
		name           string
		mockStatErr    error
		expectedErrMsg string
	}{
		{
			name:           "success",
			mockStatErr:    nil,
			expectedErrMsg: "",
		},
		{
			name:           "503 is returned (since stat fails)",
			mockStatErr:    errors.New("test error"),
			expectedErrMsg: fmt.Sprintf("503: not ready to serve traffic: backend for namespace '%s' not ready: test error", backend.ReadinessCheckNamespace),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			cp := newTestClientProvider()

			s := newTestServer(t, master1, hashRingMaxReplica(), cp)
			defer s.cleanup()

			backendClient := s.backendClient(backend.ReadinessCheckNamespace, true)

			mockStat := &core.BlobInfo{}
			if tc.mockStatErr != nil {
				mockStat = nil
			}
			backendClient.EXPECT().Stat(backend.ReadinessCheckNamespace, backend.ReadinessCheckName).Return(mockStat, tc.mockStatErr)

			err := cp.Provide(master1).CheckReadiness()
			if tc.expectedErrMsg == "" {
				require.Nil(err)
			} else {
				require.True(strings.Contains(err.Error(), tc.expectedErrMsg))
			}
		})
	}
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

	backendClient := s.backendClient(namespace, false)

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

	require.NoError(client.TransferBlob(blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content))))

	ensureHasBlob(t, cp.Provide(s.host), namespace, blob)

	bi, err := cp.Provide(master1).Stat(namespace, blob.Digest)
	require.NoError(err)
	require.NotNil(bi)
	require.Equal(int64(256), bi.Size)
}

func TestPrefetchHandler(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	client := cp.Provide(s.host)
	blob := core.SizedBlobFixture(256, 8)
	namespace := core.TagFixture()

	require.NoError(client.TransferBlob(blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content))))

	ensureHasBlob(t, cp.Provide(s.host), namespace, blob)

	err := cp.Provide(master1).PrefetchBlob(namespace, blob.Digest)
	require.NoError(err)
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

	backendClient := s.backendClient(namespace, false)
	backendClient.EXPECT().Stat(namespace, d.Hex()).Return(nil, backenderrors.ErrBlobNotFound)

	err := cp.Provide(master1).DownloadBlob(context.Background(), namespace, d, io.Discard)
	require.Error(err)
	statusErr, ok := err.(httputil.StatusError)
	require.True(ok, "expected httputil.StatusError")
	require.Equal(http.StatusNotFound, statusErr.Status)
}

func TestDeleteBlob(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()

	s := newTestServer(t, master1, hashRingMaxReplica(), cp)
	defer s.cleanup()

	client := cp.Provide(s.host)

	blob := core.NewBlobFixture()
	namespace := core.TagFixture()

	require.NoError(client.TransferBlob(blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content))))

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

	backendClient := s1.backendClient(namespace, false)
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

	backendClient := s.backendClient(namespace, false)
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

	err := cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content)))
	require.NoError(err)
	ensureHasBlob(t, cp.Provide(master1), namespace, blob)

	// Ensure metainfo was generated.
	var tm metadata.TorrentMeta
	ok, err := s.tieredStore.GetMetadata(blob.Digest.Hex(), &tm)
	require.NoError(err)
	require.True(ok)

	// Pushing again should be a no-op.
	err = cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content)))
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
			fmt.Sprintf("http://%s/internal/blobs/%s/uploads?size=1", s.addr, d.String()))
		require.NoError(err)
		_, err = httputil.Patch(
			fmt.Sprintf("http://%s/internal/blobs/foo/uploads/bar", s.addr),
			httputil.SendHeaders(map[string]string{
				"Content-Range": fmt.Sprintf("%d-%d", 0, 0),
			}))
		require.Error(err)
		require.True(httputil.IsStatus(err, http.StatusBadRequest))
	})
	t.Run("CommitInvalidDigest", func(t *testing.T) {
		require := require.New(t)

		cp := newTestClientProvider()
		s := newTestServer(t, master1, hashRingMaxReplica(), cp)
		defer s.cleanup()

		d := core.DigestFixture()
		_, err := httputil.Post(
			fmt.Sprintf("http://%s/internal/blobs/%s/uploads?size=1", s.addr, d.String()))
		require.NoError(err)

		_, err = httputil.Put(
			fmt.Sprintf("http://%s/internal/blobs/foo/uploads/bar", s.addr))
		require.Error(err)
		require.True(httputil.IsStatus(err, http.StatusBadRequest))
	})
}

func TestTransferBlobSmallChunkSize(t *testing.T) {
	require := require.New(t)

	s := newTestServer(t, master1, hashRingMaxReplica(), newTestClientProvider())
	defer s.cleanup()

	blob := core.SizedBlobFixture(1000, 1)
	namespace := core.TagFixture()

	client := blobclient.New(s.addr, blobclient.WithChunkSize(13))

	err := client.TransferBlob(blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content)))
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

	err := cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content)))
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

	require.NoError(cp.Provide(master1).TransferBlob(blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content))))

	remote := "remote:80"

	remoteCluster := s.expectRemoteCluster(remote)
	remoteCluster.EXPECT().UploadBlob(
		gomock.Any(), namespace, blob.Digest, mockutil.MatchReader(blob.Content), uint64(len(blob.Content))).Return(nil)

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

	backendClient := s.backendClient(namespace, false)
	backendClient.EXPECT().Stat(namespace,
		blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil).AnyTimes()
	backendClient.EXPECT().Download(namespace, blob.Digest.Hex(), mockutil.MatchWriter(blob.Content)).Return(nil)

	remote := "remote:80"

	remoteCluster := s.expectRemoteCluster(remote)
	remoteCluster.EXPECT().UploadBlob(
		gomock.Any(), namespace, blob.Digest, mockutil.MatchReader(blob.Content), uint64(len(blob.Content))).Return(nil)

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

	err := cp.Provide(s1.host).UploadBlob(context.Background(), namespace, blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content)))
	require.NoError(err)

	ensureHasBlob(t, cp.Provide(s1.host), namespace, blob)
	ensureHasBlob(t, cp.Provide(s2.host), namespace, blob)
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
	err := cp.Provide(s.host).UploadBlob(context.Background(), namespace, blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content)))
	require.Error(err)
	ensureHasBlob(t, cp.Provide(s.host), namespace, blob)

	// Uploading again should succeed.
	err = cp.Provide(s.host).UploadBlob(context.Background(), namespace, blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content)))
	require.NoError(err)
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

	err := cp.Provide(s.host).UploadBlob(context.Background(), namespace, blob.Digest, bytes.NewReader(blob.Content), uint64(len(blob.Content)))
	require.NoError(err)

	ensureHasBlob(t, cp.Provide(s.host), namespace, blob)
}

func newTestDiskStore(t *testing.T) *disk.Store {
	d, err := disk.NewStore(&disk.Config{
		Capacity:    100,
		RootDir:     t.TempDir(),
		ShardLength: 2,
	}, tally.NoopScope, clock.New(),
	)
	require.NoError(t, err)
	return d
}

func TestForceCleanupV2MigrationNotDone(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()
	s := newTestServerWithDiskStore(t, master1, hashRingMaxReplica(), cp, nil)
	defer s.cleanup()

	_, err := httputil.Post(fmt.Sprintf(
		"http://%s/forcecleanup/v2?target_util_percent=50&respect_eviction_ban=true", s.addr))
	require.True(httputil.IsStatus(err, http.StatusNotImplemented))
}

func TestForceCleanupV2InvalidParams(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"missing target_util_percent", "respect_eviction_ban=true"},
		{"invalid target_util_percent", "target_util_percent=abc&respect_eviction_ban=true"},
		{"missing respect_eviction_ban", "target_util_percent=50"},
		{"invalid respect_eviction_ban", "target_util_percent=50&respect_eviction_ban=abc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			cp := newTestClientProvider()
			s := newTestServer(t, master1, hashRingMaxReplica(), cp)
			defer s.cleanup()

			_, err := httputil.Post(fmt.Sprintf("http://%s/forcecleanup/v2?%s", s.addr, tc.query))
			require.True(httputil.IsStatus(err, http.StatusBadRequest))
		})
	}
}

func TestForceCleanupV2(t *testing.T) {
	require := require.New(t)

	cp := newTestClientProvider()
	diskStore := newTestDiskStore(t)
	s := newTestServerWithDiskStore(t, master1, hashRingMaxReplica(), cp, diskStore)
	defer s.cleanup()

	key := core.DigestFixture().Hex()
	f, err := diskStore.Create(key, 60)
	require.NoError(err)
	require.NoError(f.Close())
	require.NoError(diskStore.MarkComplete(key))

	// Target of 10% cannot be met while keeping the only blob (60% util), so it gets evicted.
	resp, err := httputil.Post(fmt.Sprintf(
		"http://%s/forcecleanup/v2?target_util_percent=10&respect_eviction_ban=true", s.addr))
	require.NoError(err)
	defer func() { require.NoError(resp.Body.Close()) }()

	var body map[string]int
	require.NoError(json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(0, body["new_util"])
	require.Empty(diskStore.List())
}
