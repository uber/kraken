package storage

import (
	"os"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/stretchr/testify/require"
)

func TestOriginTorrentArchiveStatNoExistTriggersRefresh(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newOriginMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive()

	blob := core.SizedBlobFixture(100, pieceLength)

	mocks.backendClient.EXPECT().Download(blob.Digest.Hex(), rwutil.MatchWriter(blob.Content))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := archive.Stat(namespace, blob.Digest.Hex())
		return err == nil
	}))

	info, err := archive.Stat(namespace, blob.Digest.Hex())
	require.NoError(err)
	require.Equal(blob.Digest.Hex(), info.Name())
	require.Equal(blob.MetaInfo.InfoHash, info.InfoHash())
	require.Equal(100, info.PercentDownloaded())
}

func TestOriginTorrentArchiveGetTorrentNoExistTriggersRefresh(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newOriginMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive()

	blob := core.SizedBlobFixture(100, pieceLength)

	mocks.backendClient.EXPECT().Download(blob.Digest.Hex(), rwutil.MatchWriter(blob.Content))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := archive.GetTorrent(namespace, blob.Digest.Hex())
		return err == nil
	}))

	tor, err := archive.GetTorrent(namespace, blob.Digest.Hex())
	require.NoError(err)
	require.Equal(blob.Digest.Hex(), tor.Name())
	require.Equal(blob.MetaInfo.InfoHash, tor.InfoHash())
	require.True(tor.Complete())
}

func TestOriginTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newOriginMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive()

	blob := core.SizedBlobFixture(100, pieceLength)

	mocks.backendClient.EXPECT().Download(blob.Digest.Hex(), rwutil.MatchWriter(blob.Content))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := archive.Stat(namespace, blob.Digest.Hex())
		return err == nil
	}))

	require.NoError(archive.DeleteTorrent(blob.Digest.Hex()))

	_, err := mocks.fs.GetCacheFileStat(blob.Digest.Hex())
	require.True(os.IsNotExist(err))
}
