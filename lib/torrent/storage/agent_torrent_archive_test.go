package storage

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/backoff"
	"github.com/stretchr/testify/require"
)

func TestAgentTorrentArchiveStatBitfield(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{})

	tf := torlib.CustomTestTorrentFileFixture(4, 1)
	mi := tf.MetaInfo

	mocks.metaInfoClient.EXPECT().Download("noexist", mi.Name()).Return(mi, nil).Times(1)

	tor, err := archive.GetTorrent(mi.Name())
	require.NoError(tor.WritePiece(tf.Content[2:3], 2))

	info, err := archive.Stat(mi.Name())
	require.NoError(err)
	require.Equal(Bitfield{false, false, true, false}, info.Bitfield())
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

func TestAgentTorrentArchiveGetTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{})

	mi := torlib.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download("noexist", mi.Name()).Return(mi, nil)

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

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{})

	mi := torlib.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download("noexist", mi.Name()).Return(nil, metainfoclient.ErrNotFound)

	tor, err := archive.GetTorrent(mi.Name())
	require.Error(err)
	require.Nil(tor)
	require.True(os.IsNotExist(archive.DeleteTorrent(mi.Name())))
}

func TestAgentTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{})

	mi := torlib.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download("noexist", mi.Name()).Return(mi, nil)

	tor, err := archive.GetTorrent(mi.Name())
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
	mocks.metaInfoClient.EXPECT().Download("noexist", mi.Name()).Return(mi, nil).AnyTimes()

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

func TestAgentTorrentArchiveDownloadMetaInfoRetryTimeout(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	config := AgentTorrentArchiveConfig{
		DownloadMetaInfoTimeout: 5 * time.Second,
		DownloadMetaInfoBackoff: backoff.Config{
			Min:    1 * time.Second,
			Max:    10 * time.Second,
			Factor: 2,
		},
	}
	archive := mocks.newTorrentArchive(config)

	name := torlib.MetaInfoFixture().Name()

	mocks.metaInfoClient.EXPECT().Download("noexist", name).Return(nil, metainfoclient.ErrRetry).AnyTimes()

	var elapsed time.Duration
	errc := make(chan error)
	go func() {
		start := time.Now()
		_, err := archive.GetTorrent(name)
		elapsed = time.Since(start)
		errc <- err
	}()

	select {
	case err := <-errc:
		require.Error(err)
		require.InDelta(config.DownloadMetaInfoTimeout, 500*time.Millisecond, float64(elapsed))
	case <-time.After(2 * config.DownloadMetaInfoTimeout):
		require.Fail("Download did not timeout")
	}
}

func TestAgentTorrentArchiveDownloadMetaInfoNonRetryErrorsFailFast(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newAgentMocks(t)
	defer cleanup()

	archive := mocks.newTorrentArchive(AgentTorrentArchiveConfig{
		DownloadMetaInfoTimeout: 5 * time.Second,
	})

	name := torlib.MetaInfoFixture().Name()

	mocks.metaInfoClient.EXPECT().Download("noexist", name).Return(nil, errors.New("some error")).AnyTimes()

	start := time.Now()
	_, err := archive.GetTorrent(name)
	elapsed := time.Since(start)
	require.Error(err)
	require.True(elapsed < time.Second)
}
