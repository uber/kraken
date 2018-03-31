package blobrefresh

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
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

const namespace = "test-namespace"

func TestRefresh(t *testing.T) {
	require := require.New(t)

	fs, cleanup := store.OriginFileStoreFixture(clock.New())
	defer cleanup()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mockbackend.NewMockClient(ctrl)

	backends, err := backend.NewManager(nil, nil)
	require.NoError(err)

	backends.Register(namespace, mockClient)

	pieceLength := 10

	refresher := New(tally.NoopScope, fs, backends, metainfogen.Fixture(fs, pieceLength))

	blob := core.SizedBlobFixture(100, uint64(pieceLength))

	mockClient.EXPECT().Download(blob.Digest.Hex(), rwutil.MatchWriter(blob.Content)).Return(nil)

	require.NoError(refresher.Refresh(namespace, blob.Digest))

	require.NoError(testutil.PollUntilTrue(5*time.Second, func() bool {
		_, err := fs.GetCacheFileStat(blob.Digest.Hex())
		return !os.IsNotExist(err)
	}))

	f, err := fs.GetCacheFileReader(blob.Digest.Hex())
	require.NoError(err)
	result, err := ioutil.ReadAll(f)
	require.Equal(string(blob.Content), string(result))

	raw, err := fs.GetCacheFileMetadata(blob.Digest.Hex(), store.NewTorrentMeta())
	require.NoError(err)
	mi, err := core.DeserializeMetaInfo(raw)
	require.NoError(err)
	require.Equal(blob.MetaInfo, mi)
}
