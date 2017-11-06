package storage

import (
	"os"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"github.com/stretchr/testify/require"
)

func TestLocalTorrentArchiveGetTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive()

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
	require.Equal(miExpected, string(miRaw))

	// Get again reads from disk.
	tor, err = archive.GetTorrent(mi.Name())
	require.NoError(err)
	require.NotNil(tor)
}

func TestLocalTorrentArchiveGetTorrentAndDeleteTorrentNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive()

	mi := torlib.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download(mi.Name()).Return(nil, metainfoclient.ErrNotFound)

	tor, err := archive.GetTorrent(mi.Name())
	require.Error(err)
	require.True(os.IsNotExist(err))
	require.Nil(tor)
	require.True(os.IsNotExist(archive.DeleteTorrent(mi.Name())))
}

func TestLocalTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive()

	mi := torlib.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download(mi.Name()).Return(mi, nil)

	tor, err := archive.GetTorrent(mi.Name())
	require.NoError(err)
	require.NotNil(tor)

	require.NoError(archive.DeleteTorrent(mi.Name()))
}

func TestLocalTorrentArchiveConcurrentGet(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newTorrentArchiveMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive()

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
