package agentstorage

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/lib/torrent/storage/piecereader"
	"code.uber.internal/infra/kraken/mocks/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/bitsetutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const namespace = "test-namespace"

const pieceLength = 4

type archiveMocks struct {
	fs             store.FileStore
	metaInfoClient *mockmetainfoclient.MockClient
}

func newArchiveMocks(t *testing.T) (*archiveMocks, func()) {
	var cleanup testutil.Cleanup

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	fs, c := store.LocalFileStoreFixture()
	cleanup.Add(c)

	metaInfoClient := mockmetainfoclient.NewMockClient(ctrl)

	return &archiveMocks{fs, metaInfoClient}, cleanup.Run
}

func (m *archiveMocks) new(config Config) *TorrentArchive {
	return NewTorrentArchive(config, tally.NoopScope, m.fs, m.metaInfoClient)
}

func TestTorrentArchiveStatBitfield(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new(Config{})

	blob := core.SizedBlobFixture(4, 1)
	mi := blob.MetaInfo

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(mi, nil).Times(1)

	tor, err := archive.CreateTorrent(namespace, mi.Name())
	require.NoError(tor.WritePiece(piecereader.NewBuffer(blob.Content[2:3]), 2))

	info, err := archive.Stat(namespace, mi.Name())
	require.NoError(err)
	require.Equal(bitsetutil.FromBools(false, false, true, false), info.Bitfield())
	require.Equal(int64(1), info.MaxPieceLength())
}

func TestTorrentArchiveStatNotExist(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new(Config{})

	name := core.MetaInfoFixture().Name()

	_, err := archive.Stat(namespace, name)
	require.True(os.IsNotExist(err))
}

func TestTorrentArchiveCreateTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new(Config{})

	mi := core.MetaInfoFixture()

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

func TestTorrentArchiveCreateTorrentNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new(Config{})

	mi := core.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(nil, metainfoclient.ErrNotFound)

	_, err := archive.CreateTorrent(namespace, mi.Name())
	require.Equal(storage.ErrNotFound, err)
}

func TestTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new(Config{})

	mi := core.MetaInfoFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(mi, nil)

	tor, err := archive.CreateTorrent(namespace, mi.Name())
	require.NoError(err)
	require.NotNil(tor)

	require.NoError(archive.DeleteTorrent(mi.Name()))

	_, err = archive.Stat(namespace, mi.Name())
	require.True(os.IsNotExist(err))
}

func TestTorrentArchiveConcurrentGet(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new(Config{})

	mi := core.MetaInfoFixture()

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

func TestTorrentArchiveGetTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new(Config{})

	mi := core.MetaInfoFixture()

	// Since metainfo is not yet on disk, get should fail.
	_, err := archive.GetTorrent(namespace, mi.Name())
	require.Error(err)

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(mi, nil)

	_, err = archive.CreateTorrent(namespace, mi.Name())
	require.NoError(err)

	// After creating the torrent, get should succeed.
	tor, err := archive.GetTorrent(namespace, mi.Name())
	require.NoError(err)
	require.NotNil(tor)
}

func TestTorrentArchiveCreateTorrentUnavailableMetaInfoRetry(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	config := Config{
		UnavailableMetaInfoRetries:    3,
		UnavailableMetaInfoRetrySleep: 200 * time.Millisecond,
	}
	archive := mocks.new(config)

	blob := core.SizedBlobFixture(1, 1)
	mi := blob.MetaInfo

	downloadErr := errors.New("something offline")

	gomock.InOrder(
		mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(nil, downloadErr),
		mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(nil, downloadErr),
		mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(mi, nil),
	)

	start := time.Now()
	tor, err := archive.CreateTorrent(namespace, mi.Name())
	require.NoError(err)
	require.NotNil(tor)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}

func TestTorrentArchiveCreateTorrentUnavailableMetaInfoRetryFailure(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	config := Config{
		UnavailableMetaInfoRetries:    3,
		UnavailableMetaInfoRetrySleep: 200 * time.Millisecond,
	}
	archive := mocks.new(config)

	blob := core.SizedBlobFixture(1, 1)
	mi := blob.MetaInfo

	mocks.metaInfoClient.EXPECT().Download(
		namespace, mi.Name()).Return(nil, errors.New("something offline")).Times(3)

	start := time.Now()
	_, err := archive.CreateTorrent(namespace, mi.Name())
	require.Error(err)
	require.InDelta(400*time.Millisecond, time.Since(start), float64(50*time.Millisecond))
}
