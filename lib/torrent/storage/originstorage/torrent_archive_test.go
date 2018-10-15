package originstorage

import (
	"os"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/metainfogen"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/utils/mockutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const pieceLength = 4

type archiveMocks struct {
	cas           *store.CAStore
	backendClient *mockbackend.MockClient
	blobRefresher *blobrefresh.Refresher
}

func newArchiveMocks(t *testing.T, namespace string) (*archiveMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	cas, c := store.CAStoreFixture()
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	backendClient := mockbackend.NewMockClient(ctrl)
	backends := backend.ManagerFixture()
	backends.Register(namespace, backendClient)

	blobRefresher := blobrefresh.New(
		blobrefresh.Config{}, tally.NoopScope, cas, backends, metainfogen.Fixture(cas, pieceLength))

	return &archiveMocks{cas, backendClient, blobRefresher}, cleanup.Run
}

func (m *archiveMocks) new() *TorrentArchive {
	return NewTorrentArchive(m.cas, m.blobRefresher)
}

func TestTorrentArchiveStatNoExistTriggersRefresh(t *testing.T) {
	require := require.New(t)

	namespace := core.TagFixture()
	mocks, cleanup := newArchiveMocks(t, namespace)
	defer cleanup()

	archive := mocks.new()

	blob := core.SizedBlobFixture(100, pieceLength)

	mocks.backendClient.EXPECT().Stat(
		blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil)
	mocks.backendClient.EXPECT().Download(blob.Digest.Hex(), mockutil.MatchWriter(blob.Content))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := archive.Stat(namespace, blob.Digest)
		return err == nil
	}))

	info, err := archive.Stat(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(blob.Digest, info.Digest())
	require.Equal(blob.MetaInfo.InfoHash(), info.InfoHash())
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
		blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil)
	mocks.backendClient.EXPECT().Download(blob.Digest.Hex(), mockutil.MatchWriter(blob.Content))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := archive.GetTorrent(namespace, blob.Digest)
		return err == nil
	}))

	tor, err := archive.GetTorrent(namespace, blob.Digest)
	require.NoError(err)
	require.Equal(blob.Digest, tor.Digest())
	require.Equal(blob.MetaInfo.InfoHash(), tor.InfoHash())
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
		blob.Digest.Hex()).Return(core.NewBlobInfo(int64(len(blob.Content))), nil)
	mocks.backendClient.EXPECT().Download(blob.Digest.Hex(), mockutil.MatchWriter(blob.Content))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := archive.Stat(namespace, blob.Digest)
		return err == nil
	}))

	require.NoError(archive.DeleteTorrent(blob.Digest))

	_, err := mocks.cas.GetCacheFileStat(blob.Digest.Hex())
	require.True(os.IsNotExist(err))
}
