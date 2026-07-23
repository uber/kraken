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
	pather := newPather(rootDir)
	md := metadata.NewTorrentMeta(core.MetaInfoFixture())
	key := "8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"

	t.Run("incomplete entry", func(t *testing.T) {
		require := require.New(t)
		complete := false
		dirPath := pather.dirPath(key, complete)
		wantDirPath := rootDir + "/incomplete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"
		require.Equal(wantDirPath, dirPath)
		blobPath := pather.blobPath(key, complete)
		wantBlobPath := rootDir + "/incomplete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data"
		require.Equal(wantBlobPath, blobPath)
		sidecarFilePath := pather.sidecarFilePath(key, complete, md.GetSuffix())
		wantSidecarFilePath := rootDir + "/incomplete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/_torrentmeta"
		require.Equal(wantSidecarFilePath, sidecarFilePath)
	})

	t.Run("complete entry", func(t *testing.T) {
		require := require.New(t)
		complete := true
		dirPath := pather.dirPath(key, complete)
		wantDirPath := rootDir + "/complete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"
		require.Equal(wantDirPath, dirPath)
		blobPath := pather.blobPath(key, complete)
		wantBlobPath := rootDir + "/complete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data"
		require.Equal(wantBlobPath, blobPath)
		sidecarFilePath := pather.sidecarFilePath(key, complete, md.GetSuffix())
		wantSidecarFilePath := rootDir + "/complete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/_torrentmeta"
		require.Equal(wantSidecarFilePath, sidecarFilePath)
	})
}
