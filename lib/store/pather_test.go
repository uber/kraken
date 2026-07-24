package store

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/metadata"
)

func TestPather(t *testing.T) {
	rootDir, err := os.MkdirTemp("/tmp", "kraken-disk-store")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(rootDir) })
	shardLength := 2
	shardedPather := newPather(rootDir, shardLength)
	unshardedPather := newPather(rootDir, 0)
	md := metadata.NewTorrentMeta(core.MetaInfoFixture())
	key := "8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"

	t.Run("incomplete entry, sharded", func(t *testing.T) {
		require := require.New(t)
		require.Equal(rootDir+"/incomplete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78", shardedPather.dirPath(key, _incompleteBlob))
		require.Equal(rootDir+"/incomplete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data", shardedPather.blobPath(key, _incompleteBlob))
		require.Equal(rootDir+"/incomplete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/_torrentmeta", shardedPather.sidecarFilePath(key, _incompleteBlob, md.GetSuffix()))
	})
	t.Run("incomplete entry, unsharded", func(t *testing.T) {
		require := require.New(t)
		require.Equal(rootDir+"/incomplete/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78", unshardedPather.dirPath(key, _incompleteBlob))
		require.Equal(rootDir+"/incomplete/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data", unshardedPather.blobPath(key, _incompleteBlob))
		require.Equal(rootDir+"/incomplete/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/_torrentmeta", unshardedPather.sidecarFilePath(key, _incompleteBlob, md.GetSuffix()))
	})

	t.Run("complete entry, sharded", func(t *testing.T) {
		require := require.New(t)
		require.Equal(rootDir+"/complete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78", shardedPather.dirPath(key, _completeBlob))
		require.Equal(rootDir+"/complete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data", shardedPather.blobPath(key, _completeBlob))
		require.Equal(rootDir+"/complete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/_torrentmeta", shardedPather.sidecarFilePath(key, _completeBlob, md.GetSuffix()))
	})
	t.Run("complete entry, unsharded", func(t *testing.T) {
		require := require.New(t)
		require.Equal(rootDir+"/complete/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78", unshardedPather.dirPath(key, _completeBlob))
		require.Equal(rootDir+"/complete/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data", unshardedPather.blobPath(key, _completeBlob))
		require.Equal(rootDir+"/complete/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/_torrentmeta", unshardedPather.sidecarFilePath(key, _completeBlob, md.GetSuffix()))
	})

	t.Run("unsharded entries with non-digest keys", func(t *testing.T) {
		key := "foo"
		require := require.New(t)
		require.Equal(rootDir+"/complete/foo", unshardedPather.dirPath(key, _completeBlob))
		require.Equal(rootDir+"/complete/foo/data", unshardedPather.blobPath(key, _completeBlob))
		require.Equal(rootDir+"/complete/foo/_torrentmeta", unshardedPather.sidecarFilePath(key, _completeBlob, md.GetSuffix()))
	})
}
