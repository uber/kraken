package storage

import (
	"os"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"github.com/stretchr/testify/require"
)

func TestAgentTorrentArchiveStatBitfield(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newAgentTorrentArchive()

	tf := torlib.CustomTestTorrentFileFixture(4, 1)
	mi := tf.MetaInfo

	mocks.metaInfoClient.EXPECT().Download(mi.Name()).Return(mi, nil).Times(1)

	tor, err := archive.GetTorrent(mi.Name())
	require.NoError(tor.WritePiece(tf.Content[2:3], 2))

	info, err := archive.Stat(mi.Name())
	require.NoError(err)
	require.Equal(Bitfield{false, false, true, false}, info.Bitfield())
	require.Equal(int64(1), info.MaxPieceLength())
}

func TestAgentTorrentArchiveStatNotExist(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newAgentTorrentArchive()

	name := torlib.MetaInfoFixture().Name()

	_, err := archive.Stat(name)
	require.Error(err)
}

func TestAgentTorrentArchiveGetTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newAgentTorrentArchive()

	mi := torlib.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download(mi.Name()).Return(mi, nil)

	tor, err := archive.GetTorrent(mi.Name())
	require.NoError(err)
	require.NotNil(tor)

	// Check metainfo.
	miRaw, err := mocks.fs.GetDownloadOrCacheFileMeta(mi.Name())
	require.NoError(err)
	miExpected, err := mi.Serialize()
	require.NoError(err)
	require.Equal(string(miExpected), string(miRaw))

	// Get again reads from disk.
	tor, err = archive.GetTorrent(mi.Name())
	require.NoError(err)
	require.NotNil(tor)
}

func TestAgentTorrentArchiveGetTorrentAndDeleteTorrentNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newAgentTorrentArchive()

	mi := torlib.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download(mi.Name()).Return(nil, metainfoclient.ErrNotFound)

	tor, err := archive.GetTorrent(mi.Name())
	require.Error(err)
	require.Nil(tor)
	require.True(os.IsNotExist(archive.DeleteTorrent(mi.Name())))
}

func TestAgentTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newAgentTorrentArchive()

	mi := torlib.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download(mi.Name()).Return(mi, nil)

	tor, err := archive.GetTorrent(mi.Name())
	require.NoError(err)
	require.NotNil(tor)

	require.NoError(archive.DeleteTorrent(mi.Name()))
}

func TestAgentTorrentArchiveConcurrentGet(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newAgentTorrentArchive()

	mi := torlib.MetaInfoFixture()

	// Allow any times for concurrency below.
	mocks.metaInfoClient.EXPECT().Download(mi.Name()).Return(mi, nil).AnyTimes()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tor, err := archive.GetTorrent(mi.Name())
			require.NoError(err)
			require.NotNil(tor)
		}()
	}
	wg.Wait()
}
