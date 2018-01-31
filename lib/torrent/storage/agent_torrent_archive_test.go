package storage

import (
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"github.com/stretchr/testify/require"
)

const namespace = "test-namespace"

func TestAgentTorrentArchiveStatBitfield(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{})

	tf := torlib.CustomTestTorrentFileFixture(4, 1)
	mi := tf.MetaInfo

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(mi, nil).Times(1)

	tor, err := archive.CreateTorrent(namespace, mi.Name())
	require.NoError(tor.WritePiece(tf.Content[2:3], 2))

	info, err := archive.Stat(mi.Name())
	require.NoError(err)
	require.Equal(BitSetFixture(false, false, true, false), info.Bitfield())
	require.Equal(int64(1), info.MaxPieceLength())
}

func TestAgentTorrentArchiveStatNotExist(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{})

	name := torlib.MetaInfoFixture().Name()

	_, err := archive.Stat(name)
	require.Error(err)
}

func TestAgentTorrentArchiveCreateTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{})

	mi := torlib.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(mi, nil)

	tor, err := archive.CreateTorrent(namespace, mi.Name())
	require.NoError(err)
	require.NotNil(tor)

	// Check metainfo.
	miRaw, err := mocks.fs.GetDownloadOrCacheFileMeta(mi.Name())
	require.NoError(err)
	miExpected, err := mi.Serialize()
	require.NoError(err)
	require.Equal(string(miExpected), string(miRaw))

	// Create again reads from disk.
	tor, err = archive.CreateTorrent(namespace, mi.Name())
	require.NoError(err)
	require.NotNil(tor)
}

func TestAgentTorrentArchiveCreateTorrentNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{})

	mi := torlib.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(nil, metainfoclient.ErrNotFound)

	_, err := archive.CreateTorrent(namespace, mi.Name())
	require.Equal(ErrNotFound, err)
}

func TestAgentTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{})

	mi := torlib.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(mi, nil)

	tor, err := archive.CreateTorrent(namespace, mi.Name())
	require.NoError(err)
	require.NotNil(tor)

	require.NoError(archive.DeleteTorrent(mi.Name()))
}

func TestAgentTorrentArchiveConcurrentGet(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{})

	mi := torlib.MetaInfoFixture()

	// Allow any times for concurrency below.
	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(mi, nil).AnyTimes()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tor, err := archive.CreateTorrent(namespace, mi.Name())
			require.NoError(err)
			require.NotNil(tor)
		}()
	}
	wg.Wait()
}

func TestAgentTorrentArchiveGetTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{})

	mi := torlib.MetaInfoFixture()

	// Since metainfo is not yet on disk, get should fail.
	_, err := archive.GetTorrent(mi.Name())
	require.Error(err)

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(mi, nil)

	_, err = archive.CreateTorrent(namespace, mi.Name())
	require.NoError(err)

	// After creating the torrent, get should succeed.
	tor, err := archive.GetTorrent(mi.Name())
	require.NoError(err)
	require.NotNil(tor)
}
