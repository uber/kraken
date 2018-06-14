package originstorage

import (
	"os"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/blobinfo"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/metainfogen"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const pieceLength = 4

type archiveMocks struct {
	fs            store.OriginFileStore
	backendClient *mockbackend.MockClient
	blobRefresher *blobrefresh.Refresher
}

func newArchiveMocks(t *testing.T, namespace string) (*archiveMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	fs, c := store.OriginFileStoreFixture(clock.New())
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	backendClient := mockbackend.NewMockClient(ctrl)
	backends := backend.ManagerFixture()
	backends.Register(namespace, backendClient)

	blobRefresher := blobrefresh.New(
		blobrefresh.Config{}, tally.NoopScope, fs, backends, metainfogen.Fixture(fs, pieceLength))

	return &archiveMocks{fs, backendClient, blobRefresher}, cleanup.Run
}

func (m *archiveMocks) new() *TorrentArchive {
	return NewTorrentArchive(m.fs, m.blobRefresher)
}

func TestTorrentArchiveStatNoExistTriggersRefresh(t *testing.T) {
	require := require.New(t)

	namespace := core.TagFixture()
	mocks, cleanup := newArchiveMocks(t, namespace)
	defer cleanup()

	archive := mocks.new()

	blob := core.SizedBlobFixture(100, pieceLength)

	mocks.backendClient.EXPECT().Stat(
		blob.Digest.Hex()).Return(blobinfo.New(int64(len(blob.Content))), nil)
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

func TestTorrentArchiveGetTorrentNoExistTriggersRefresh(t *testing.T) {
	require := require.New(t)

	namespace := core.TagFixture()
	mocks, cleanup := newArchiveMocks(t, namespace)
	defer cleanup()

	archive := mocks.new()

	blob := core.SizedBlobFixture(100, pieceLength)

	mocks.backendClient.EXPECT().Stat(
		blob.Digest.Hex()).Return(blobinfo.New(int64(len(blob.Content))), nil)
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

func TestTorrentArchiveDeleteTorrent(t *testing.T) {
	require := require.New(t)

	namespace := core.TagFixture()
	mocks, cleanup := newArchiveMocks(t, namespace)
	defer cleanup()

	archive := mocks.new()

	blob := core.SizedBlobFixture(100, pieceLength)

	mocks.backendClient.EXPECT().Stat(
		blob.Digest.Hex()).Return(blobinfo.New(int64(len(blob.Content))), nil)
	mocks.backendClient.EXPECT().Download(blob.Digest.Hex(), rwutil.MatchWriter(blob.Content))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := archive.Stat(namespace, blob.Digest.Hex())
		return err == nil
	}))

	require.NoError(archive.DeleteTorrent(blob.Digest.Hex()))

	_, err := mocks.fs.GetCacheFileStat(blob.Digest.Hex())
	require.True(os.IsNotExist(err))
}
