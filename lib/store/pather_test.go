package store

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store/metadata"
	"github.com/uber/kraken/utils/memsize"
)

func TestPather(t *testing.T) {
	require := require.New(t)
	store, rootDir := newTestStore(t, 10*memsize.KB)
	md := metadata.NewTorrentMeta(core.MetaInfoFixture())

	key := "8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"
	complete := false
	dirPath := store.dirPath(key, complete)
	wantDirPath := rootDir + "/incomplete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"
	require.Equal(wantDirPath, dirPath)
	blobPath := store.blobPath(key, complete)
	wantBlobPath := rootDir + "/incomplete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data"
	require.Equal(wantBlobPath, blobPath)
	sidecarFilePath := store.sidecarFilePath(key, complete, md.GetSuffix())
	wantSidecarFilePath := rootDir + "/incomplete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/_torrentmeta"
	require.Equal(wantSidecarFilePath, sidecarFilePath)

	complete = true
	dirPath = store.dirPath(key, complete)
	wantDirPath = rootDir + "/complete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78"
	require.Equal(wantDirPath, dirPath)
	blobPath = store.blobPath(key, complete)
	wantBlobPath = rootDir + "/complete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/data"
	require.Equal(wantBlobPath, blobPath)
	sidecarFilePath = store.sidecarFilePath(key, complete, md.GetSuffix())
	wantSidecarFilePath = rootDir + "/complete/8c/6a/8c6af6ca6458353bfa8cb3d756ca54a4fe7b1de04196bf1b37e0863c3f806a78/_torrentmeta"
	require.Equal(wantSidecarFilePath, sidecarFilePath)
}
