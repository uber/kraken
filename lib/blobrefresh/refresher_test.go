package blobrefresh

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/blobinfo"
	"code.uber.internal/infra/kraken/lib/metainfogen"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/store/metadata"
	"code.uber.internal/infra/kraken/mocks/lib/backend"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/andres-erbsen/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const _testPieceLength = 10

type refresherMocks struct {
	ctrl     *gomock.Controller
	fs       store.OriginFileStore
	backends *backend.Manager
	config   Config
}

func newRefresherMocks(t *testing.T) (*refresherMocks, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	fs, c := store.OriginFileStoreFixture(clock.New())
	cleanup.Add(c)

	ctrl := gomock.NewController(t)
	cleanup.Add(ctrl.Finish)

	backends := backend.ManagerFixture()

	return &refresherMocks{ctrl, fs, backends, Config{}}, cleanup.Run
}

func (m *refresherMocks) new() *Refresher {
	return New(m.config, tally.NoopScope, m.fs, m.backends, metainfogen.Fixture(m.fs, _testPieceLength))
}

func (m *refresherMocks) newClient(namespace string) *mockbackend.MockClient {
	client := mockbackend.NewMockClient(m.ctrl)
	m.backends.Register(namespace, client)
	return client
}

func TestRefresh(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newRefresherMocks(t)
	defer cleanup()

	refresher := mocks.new()

	namespace := core.TagFixture()
	client := mocks.newClient(namespace)

	blob := core.SizedBlobFixture(100, uint64(_testPieceLength))

	client.EXPECT().Stat(blob.Digest.Hex()).Return(blobinfo.New(int64(len(blob.Content))), nil)
	client.EXPECT().Download(blob.Digest.Hex(), rwutil.MatchWriter(blob.Content)).Return(nil)

	require.NoError(refresher.Refresh(namespace, blob.Digest))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := mocks.fs.GetCacheFileStat(blob.Digest.Hex())
		return !os.IsNotExist(err)
	}))

	f, err := mocks.fs.GetCacheFileReader(blob.Digest.Hex())
	require.NoError(err)
	result, err := ioutil.ReadAll(f)
	require.Equal(string(blob.Content), string(result))

	var tm metadata.TorrentMeta
	require.NoError(mocks.fs.GetCacheFileMetadata(blob.Digest.Hex(), &tm))
	require.Equal(blob.MetaInfo, tm.MetaInfo)
}

func TestRefreshSizeLimitError(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newRefresherMocks(t)
	defer cleanup()

	mocks.config.SizeLimit = 99

	refresher := mocks.new()

	namespace := core.TagFixture()
	client := mocks.newClient(namespace)

	blob := core.SizedBlobFixture(100, uint64(_testPieceLength))

	client.EXPECT().Stat(blob.Digest.Hex()).Return(blobinfo.New(int64(len(blob.Content))), nil)

	require.Error(refresher.Refresh(namespace, blob.Digest))
}

func TestRefreshSizeLimitWithValidSize(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newRefresherMocks(t)
	defer cleanup()

	mocks.config.SizeLimit = 100

	refresher := mocks.new()

	namespace := core.TagFixture()
	client := mocks.newClient(namespace)

	blob := core.SizedBlobFixture(100, uint64(_testPieceLength))

	client.EXPECT().Stat(blob.Digest.Hex()).Return(blobinfo.New(int64(len(blob.Content))), nil)
	client.EXPECT().Download(blob.Digest.Hex(), rwutil.MatchWriter(blob.Content)).Return(nil)

	require.NoError(refresher.Refresh(namespace, blob.Digest))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := mocks.fs.GetCacheFileStat(blob.Digest.Hex())
		return !os.IsNotExist(err)
	}))
}
