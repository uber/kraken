package storage

import (
	"os"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"
	"github.com/stretchr/testify/require"
)

func TestLocalTorrentArchiveCreateTorrent(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.LocalFileStoreFixture()
	defer cleanup()

	archive := NewLocalTorrentArchive(fs)

	mi := torlib.MetaInfoFixture()

	_, err := archive.CreateTorrent(mi)
	require.NoError(err)

	// Check metainfo.
	miRaw, err := fs.GetDownloadOrCacheFileMeta(mi.Name())
	require.NoError(err)
	miExpected, err := mi.Serialize()
	require.NoError(err)
	require.Equal(miExpected, string(miRaw))

	// Verify exist
	_, err = archive.GetTorrent(mi.Name(), mi.InfoHash)
	require.NoError(err)

	// Create again is ok
	_, err = archive.CreateTorrent(mi)
	require.NoError(err)
}

func TestLocalTorrentArchiveGetTorrentAndDeleteTorrentNotFound(t *testing.T) {
	require := require.New(t)

	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	mi := torlib.MetaInfoFixture()

	tor, err := archive.GetTorrent(mi.Name(), mi.InfoHash)
	require.Error(err)
	require.True(os.IsNotExist(err))
	require.Nil(tor)
	require.True(os.IsNotExist(archive.DeleteTorrent(mi.Name(), mi.InfoHash)))
}

func TestLocalTorrentArchiveGetTorrentInfoHashMismatch(t *testing.T) {
	require := require.New(t)

	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	mi := torlib.MetaInfoFixture()

	_, err := archive.CreateTorrent(mi)
	require.NoError(err)

	badInfoHash := torlib.NewInfoHashFromBytes([]byte{})
	tor, err := archive.GetTorrent(mi.Name(), badInfoHash)
	require.Error(err)
	require.True(IsInfoHashMismatchError(err))
	require.Equal(err.(InfoHashMismatchError).expected, badInfoHash)
	require.Equal(err.(InfoHashMismatchError).actual, mi.InfoHash)
	require.Nil(tor)
}

func TestLocalTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	mi := torlib.MetaInfoFixture()

	// Torrent exists
	_, err := archive.CreateTorrent(mi)
	require.NoError(err)

	// Verify exists
	_, err = archive.GetTorrent(mi.Name(), mi.InfoHash)
	require.NoError(err)

	// Delete torrent
	require.NoError(archive.DeleteTorrent(mi.Name(), mi.InfoHash))

	// Confirm deleted
	_, err = archive.GetTorrent(mi.Name(), mi.InfoHash)
	require.Error(err)
	require.True(os.IsNotExist(err))
}

func TestLocalTorrentArchiveConcurrentCreate(t *testing.T) {
	require := require.New(t)

	archive, cleanup := TorrentArchiveFixture()
	defer cleanup()

	mi := torlib.MetaInfoFixture()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := archive.CreateTorrent(mi)
			require.NoError(err)
		}()
	}
	wg.Wait()
}
