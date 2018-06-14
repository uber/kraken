package agentstorage

import (
	"os"
	"sync"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/store/metadata"
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

func (m *archiveMocks) new() *TorrentArchive {
	return NewTorrentArchive(tally.NoopScope, m.fs, m.metaInfoClient)
}

func TestTorrentArchiveStatBitfield(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new()

	namespace := core.TagFixture()
	blob := core.SizedBlobFixture(4, 1)
	mi := blob.MetaInfo

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(mi, nil).Times(1)

	tor, err := archive.CreateTorrent(namespace, mi.Name())
	require.NoError(err)

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

	archive := mocks.new()

	namespace := core.TagFixture()
	name := core.MetaInfoFixture().Name()

	_, err := archive.Stat(namespace, name)
	require.True(os.IsNotExist(err))
}

func TestTorrentArchiveCreateTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new()

	mi := core.MetaInfoFixture()
	namespace := core.TagFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(mi, nil)

	tor, err := archive.CreateTorrent(namespace, mi.Name())
	require.NoError(err)
	require.NotNil(tor)

	// Check metainfo.
	var tm metadata.TorrentMeta
	require.NoError(mocks.fs.States().Download().Cache().GetMetadata(mi.Name(), &tm))
	require.Equal(mi, tm.MetaInfo)

	// Create again reads from disk.
	tor, err = archive.CreateTorrent(namespace, mi.Name())
	require.NoError(err)
	require.NotNil(tor)
}

func TestTorrentArchiveCreateTorrentNotFound(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new()

	mi := core.MetaInfoFixture()
	namespace := core.TagFixture()

	mocks.metaInfoClient.EXPECT().Download(namespace, mi.Name()).Return(nil, metainfoclient.ErrNotFound)

	_, err := archive.CreateTorrent(namespace, mi.Name())
	require.Equal(storage.ErrNotFound, err)
}

func TestTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newArchiveMocks(t)
	defer cleanup()

	archive := mocks.new()

	mi := core.MetaInfoFixture()
	namespace := core.TagFixture()

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

	archive := mocks.new()

	mi := core.MetaInfoFixture()
	namespace := core.TagFixture()

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

	archive := mocks.new()

	mi := core.MetaInfoFixture()
	namespace := core.TagFixture()

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
