package blobserver

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/mocks/lib/store"
)

func TestRepairOwnedShardPushesToReplica(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider()

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	s2 := newTestServer(master2, config, cp)
	defer s2.cleanup()

	shardID := pickShard(config, master1, master2)

	// Push blobs to master1.
	var blobs []*core.BlobFixture
	for i := 0; i < 5; i++ {
		blob := computeBlobForShard(shardID)
		blobs = append(blobs, blob)

		err := cp.Provide(master1).TransferBlob(
			blob.Digest, bytes.NewReader(blob.Content), int64(len(blob.Content)))
		require.NoError(err)
	}

	_, err := cp.Provide(master1).RepairShard(shardID)
	require.NoError(err)

	// Ensure master2 received the blob.
	for _, blob := range blobs {
		ensureHasBlob(t, cp.Provide(master2), blob)
	}
}

func TestRepairUnownedShardPushesToReplicasAndDeletes(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider()

	shardID := pickShard(config, master1, master2)

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	// Push blobs to master1.
	var blobs []*core.BlobFixture
	for i := 0; i < 5; i++ {
		blob := computeBlobForShard(shardID)
		blobs = append(blobs, blob)

		err := cp.Provide(master1).TransferBlob(
			blob.Digest, bytes.NewReader(blob.Content), int64(len(blob.Content)))
		require.NoError(err)
	}

	// Remove all capacity from master1. shardID will now be owned by master2 and master3.
	config.HashNodes[master1] = HashNodeConfig{Label: "origin1", Weight: 0}
	s1.restart(config)

	s2 := newTestServer(master2, config, cp)
	defer s2.cleanup()

	s3 := newTestServer(master3, config, cp)
	defer s3.cleanup()

	_, err := cp.Provide(master1).RepairShard(shardID)
	require.NoError(err)

	for _, blob := range blobs {
		for _, master := range []string{master2, master3} {
			ensureHasBlob(t, cp.Provide(master), blob)
		}

		// Ensure master1 deleted the blobs.
		_, err := s1.fs.GetCacheFileStat(blob.Digest.Hex())
		require.Error(err)
		require.True(os.IsNotExist(err))
	}
}

func TestRepairUnownedShardDeletesIfReplicasAlreadyHaveShard(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider()

	shardID := pickShard(config, master1, master2)

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	// Push blobs to master1.
	var blobs []*core.BlobFixture
	for i := 0; i < 5; i++ {
		blob := computeBlobForShard(shardID)
		blobs = append(blobs, blob)

		err := cp.Provide(master1).TransferBlob(
			blob.Digest, bytes.NewReader(blob.Content), int64(len(blob.Content)))
		require.NoError(err)
	}

	// Remove all capacity from master1. shardID will now be owned by master2 and master3.
	config.HashNodes[master1] = HashNodeConfig{Label: "origin1", Weight: 0}
	s1.restart(config)

	s2 := newTestServer(master2, config, cp)
	defer s2.cleanup()

	s3 := newTestServer(master3, config, cp)
	defer s3.cleanup()

	// Push blobs to master2 and master3.
	for _, blob := range blobs {
		for _, m := range []string{master2, master3} {
			err := cp.Provide(m).TransferBlob(
				blob.Digest, bytes.NewReader(blob.Content), int64(len(blob.Content)))
			require.NoError(err)
		}
	}

	_, err := cp.Provide(master1).RepairShard(shardID)
	require.NoError(err)

	// Ensure master1 deleted the blobs.
	for _, blob := range blobs {
		_, err := s1.fs.GetCacheFileStat(blob.Digest.Hex())
		require.Error(err)
		require.True(os.IsNotExist(err))
	}
}

func TestRepairUnownedShardDoesNotDeleteIfReplicationFails(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider()

	shardID := pickShard(config, master1, master2)

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	// Push blobs to master1.
	var blobs []*core.BlobFixture
	for i := 0; i < 5; i++ {
		blob := computeBlobForShard(shardID)
		blobs = append(blobs, blob)

		err := cp.Provide(master1).TransferBlob(
			blob.Digest, bytes.NewReader(blob.Content), int64(len(blob.Content)))
		require.NoError(err)
	}

	// Remove all capacity from master1. shardID will now be owned by master2 and master3.
	config.HashNodes[master1] = HashNodeConfig{Label: "origin1", Weight: 0}
	s1.restart(config)

	s2 := newTestServer(master2, config, cp)
	defer s2.cleanup()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Start master3 with a "broken" file store, such that all pushes to master3 will fail.
	fs3 := mockstore.NewMockOriginFileStore(ctrl)
	fs3.EXPECT().GetCacheFileStat(gomock.Any()).MinTimes(1).Return(nil, os.ErrNotExist)
	fs3.EXPECT().CreateUploadFile(
		gomock.Any(), int64(0)).MinTimes(1).Return(errors.New("some error"))
	addr3, stop := startServer(master3, config, fs3, cp, core.PeerContextFixture(), nil)
	defer stop()
	cp.register(master3, addr3)

	_, err := cp.Provide(master1).RepairShard(shardID)
	require.NoError(err)

	for _, blob := range blobs {
		ensureHasBlob(t, cp.Provide(master2), blob)

		// Ensure master1 did not delete the blobs.
		_, err = s1.fs.GetCacheFileStat(blob.Digest.Hex())
		require.NoError(err)
	}
}

func TestRepairAllShards(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider()

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	s2 := newTestServer(master2, config, cp)
	defer s2.cleanup()

	var blobs []*core.BlobFixture
	for i := 0; i < 5; i++ {
		blob := computeBlobForHosts(config, master1, master2)
		blobs = append(blobs, blob)

		err := cp.Provide(master1).TransferBlob(
			blob.Digest, bytes.NewReader(blob.Content), int64(len(blob.Content)))
		require.NoError(err)
	}

	_, err := cp.Provide(master1).Repair()
	require.NoError(err)

	for _, blob := range blobs {
		ensureHasBlob(t, cp.Provide(master2), blob)
	}
}

func TestRepairDigest(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider()

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	s2 := newTestServer(master2, config, cp)
	defer s2.cleanup()

	blob := computeBlobForHosts(config, master1, master2)

	err := cp.Provide(master1).TransferBlob(
		blob.Digest, bytes.NewReader(blob.Content), int64(len(blob.Content)))
	require.NoError(err)

	_, err = cp.Provide(master1).RepairDigest(blob.Digest)
	require.NoError(err)

	ensureHasBlob(t, cp.Provide(master2), blob)
}
