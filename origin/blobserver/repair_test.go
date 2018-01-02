package blobserver

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/mocks/lib/store"
)

func TestRepairOwnedShardPushesToReplica(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider(clientConfigFixture())

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	s2 := newTestServer(master2, config, cp)
	defer s2.cleanup()

	shardID := pickShard(config, master1, master2)

	// Push blobs to master1.
	blobs := make(map[image.Digest][]byte)
	for i := 0; i < 5; i++ {
		d, blob := computeBlobForShard(shardID)
		blobs[d] = blob

		err := cp.Provide(master1).PushBlob(d, bytes.NewBuffer(blob), int64(len(blob)))
		require.NoError(err)
	}

	_, err := cp.Provide(master1).RepairShard(shardID)
	require.NoError(err)

	// Ensure master2 received the blob.
	for d, blob := range blobs {
		ensureHasBlob(t, s2.fs, d, blob)
	}
}

func TestRepairUnownedShardPushesToReplicasAndDeletes(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider(clientConfigFixture())

	shardID := pickShard(config, master1, master2)

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	// Push blobs to master1.
	blobs := make(map[image.Digest][]byte)
	for i := 0; i < 5; i++ {
		d, blob := computeBlobForShard(shardID)
		blobs[d] = blob

		err := cp.Provide(master1).PushBlob(d, bytes.NewBuffer(blob), int64(len(blob)))
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

	for d, blob := range blobs {
		for _, s := range []*testServer{s2, s3} {
			ensureHasBlob(t, s.fs, d, blob)
		}
		// Ensure master1 deleted the blobs.
		_, err := s1.fs.GetCacheFileStat(d.Hex())
		require.Error(err)
		require.True(os.IsNotExist(err))
	}
}

func TestRepairUnownedShardDeletesIfReplicasAlreadyHaveShard(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider(clientConfigFixture())

	shardID := pickShard(config, master1, master2)

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	// Push blobs to master1.
	blobs := make(map[image.Digest][]byte)
	for i := 0; i < 5; i++ {
		d, blob := computeBlobForShard(shardID)
		blobs[d] = blob

		err := cp.Provide(master1).PushBlob(d, bytes.NewBuffer(blob), int64(len(blob)))
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
	for d, blob := range blobs {
		for _, m := range []string{master2, master3} {
			err := cp.Provide(m).PushBlob(d, bytes.NewBuffer(blob), int64(len(blob)))
			require.NoError(err)
		}
	}

	_, err := cp.Provide(master1).RepairShard(shardID)
	require.NoError(err)

	// Ensure master1 deleted the blobs.
	for d := range blobs {
		_, err := s1.fs.GetCacheFileStat(d.Hex())
		require.Error(err)
		require.True(os.IsNotExist(err))
	}
}

func TestRepairUnownedShardDoesNotDeleteIfReplicationFails(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider(clientConfigFixture())

	shardID := pickShard(config, master1, master2)

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	// Push blobs to master1.
	blobs := make(map[image.Digest][]byte)
	for i := 0; i < 5; i++ {
		d, blob := computeBlobForShard(shardID)
		blobs[d] = blob

		err := cp.Provide(master1).PushBlob(d, bytes.NewBuffer(blob), int64(len(blob)))
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
	fs3 := mockstore.NewMockFileStore(ctrl)
	fs3.EXPECT().GetCacheFileStat(gomock.Any()).MinTimes(1).Return(nil, os.ErrNotExist)
	fs3.EXPECT().CreateUploadFile(gomock.Any(), int64(0)).MinTimes(1).Return(errors.New("some error"))
	addr3, stop := startServer(master3, config, fs3, cp, peercontext.Fixture(), nil)
	defer stop()
	cp.register(master3, addr3)

	_, err := cp.Provide(master1).RepairShard(shardID)
	require.NoError(err)

	for d, blob := range blobs {
		ensureHasBlob(t, s2.fs, d, blob)

		// Ensure master1 did not delete the blobs.
		_, err = s1.fs.GetCacheFileStat(d.Hex())
		require.NoError(err)
	}
}

func TestRepairAllShards(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider(clientConfigFixture())

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	s2 := newTestServer(master2, config, cp)
	defer s2.cleanup()

	blobs := make(map[image.Digest][]byte)
	for i := 0; i < 5; i++ {
		d, blob := computeBlobForHosts(config, master1, master2)
		blobs[d] = blob

		err := cp.Provide(master1).PushBlob(d, bytes.NewBuffer(blob), int64(len(blob)))
		require.NoError(err)
	}

	_, err := cp.Provide(master1).Repair()
	require.NoError(err)

	for d, blob := range blobs {
		ensureHasBlob(t, s2.fs, d, blob)
	}
}

func TestRepairDigest(t *testing.T) {
	require := require.New(t)

	config := configFixture()
	cp := newTestClientProvider(clientConfigFixture())

	s1 := newTestServer(master1, config, cp)
	defer s1.cleanup()

	s2 := newTestServer(master2, config, cp)
	defer s2.cleanup()

	d, blob := computeBlobForHosts(config, master1, master2)

	err := cp.Provide(master1).PushBlob(d, bytes.NewBuffer(blob), int64(len(blob)))
	require.NoError(err)

	_, err = cp.Provide(master1).RepairDigest(d)
	require.NoError(err)

	ensureHasBlob(t, s2.fs, d, blob)
}
