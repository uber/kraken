package trackerserver

import (
	"errors"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestAnnounceSinglePeerResponse(t *testing.T) {
	require := require.New(t)

	config := Config{AnnounceInterval: 5 * time.Second}

	mocks, cleanup := newServerMocks(t, config)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	blob := core.NewBlobFixture()
	pctx := core.PeerContextFixture()

	client := announceclient.Default(pctx, serverset.MustRoundRobin(addr))

	peers := []*core.PeerInfo{core.PeerInfoFixture()}

	mocks.peerStore.EXPECT().GetOrigins(blob.MetaInfo.InfoHash).Return(nil, nil)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash, gomock.Any()).Return(peers, nil)
	mocks.peerStore.EXPECT().UpdatePeer(
		blob.MetaInfo.InfoHash, core.PeerInfoFromContext(pctx, false)).Return(nil)

	result, interval, err := client.Announce(blob.MetaInfo.Name(), blob.MetaInfo.InfoHash, false)
	require.NoError(err)
	require.Equal(peers, result)
	require.Equal(config.AnnounceInterval, interval)
}

func TestAnnounceReturnsCachedOrigin(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	blob := core.NewBlobFixture()
	pctx := core.PeerContextFixture()
	octx := core.OriginContextFixture()

	client := announceclient.Default(pctx, serverset.MustRoundRobin(addr))

	origins := []*core.PeerInfo{core.PeerInfoFromContext(octx, true)}

	mocks.peerStore.EXPECT().GetOrigins(blob.MetaInfo.InfoHash).Return(origins, nil)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash, gomock.Any()).Return(nil, nil)
	mocks.peerStore.EXPECT().UpdatePeer(
		blob.MetaInfo.InfoHash, core.PeerInfoFromContext(pctx, false)).Return(nil)

	result, _, err := client.Announce(blob.MetaInfo.Name(), blob.MetaInfo.InfoHash, false)
	require.NoError(err)
	require.Equal(origins, result)
}

func TestAnnounceMissingOriginsFetchesAndCachesOrigins(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	pctx := core.PeerContextFixture()
	octx := core.OriginContextFixture()
	blob := core.NewBlobFixture()

	client := announceclient.Default(pctx, serverset.MustRoundRobin(addr))

	origins := []*core.PeerInfo{core.PeerInfoFromContext(octx, true)}

	mocks.originCluster.EXPECT().Owners(blob.Digest).Return([]core.PeerContext{octx}, nil)

	mocks.peerStore.EXPECT().UpdatePeer(
		blob.MetaInfo.InfoHash, core.PeerInfoFromContext(pctx, false)).Return(nil)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash, gomock.Any()).Return(nil, nil)
	mocks.peerStore.EXPECT().GetOrigins(
		blob.MetaInfo.InfoHash).Return(nil, storage.ErrNoOrigins)
	mocks.peerStore.EXPECT().UpdateOrigins(blob.MetaInfo.InfoHash, origins).Return(nil)

	result, _, err := client.Announce(blob.MetaInfo.Name(), blob.MetaInfo.InfoHash, false)
	require.NoError(err)
	require.Equal(origins, result)
}

func TestAnnounceUnavailablePeerStoreCanStillProvideOrigins(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	pctx := core.PeerContextFixture()
	octx := core.OriginContextFixture()
	blob := core.NewBlobFixture()

	client := announceclient.Default(pctx, serverset.MustRoundRobin(addr))

	origins := []*core.PeerInfo{core.PeerInfoFromContext(octx, true)}

	mocks.originCluster.EXPECT().Owners(blob.Digest).Return([]core.PeerContext{octx}, nil)

	storageErr := errors.New("some storage error")

	mocks.peerStore.EXPECT().UpdatePeer(
		blob.MetaInfo.InfoHash, core.PeerInfoFromContext(pctx, false)).Return(storageErr)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash, gomock.Any()).Return(nil, storageErr)
	mocks.peerStore.EXPECT().GetOrigins(blob.MetaInfo.InfoHash).Return(nil, storageErr)

	result, _, err := client.Announce(blob.MetaInfo.Name(), blob.MetaInfo.InfoHash, false)
	require.NoError(err)
	require.Equal(origins, result)
}

func TestAnnouceNoOriginsAndUnavailableOriginClusterCanStillProvidePeers(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	pctx := core.PeerContextFixture()
	blob := core.NewBlobFixture()

	client := announceclient.Default(pctx, serverset.MustRoundRobin(addr))

	peers := []*core.PeerInfo{core.PeerInfoFixture()}

	mocks.originCluster.EXPECT().Owners(blob.Digest).Return(nil, errors.New("origin cluster error"))

	mocks.peerStore.EXPECT().UpdatePeer(
		blob.MetaInfo.InfoHash, core.PeerInfoFromContext(pctx, false)).Return(nil)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash, gomock.Any()).Return(peers, nil)
	mocks.peerStore.EXPECT().GetOrigins(
		blob.MetaInfo.InfoHash).Return(nil, storage.ErrNoOrigins)

	result, _, err := client.Announce(blob.MetaInfo.Name(), blob.MetaInfo.InfoHash, false)
	require.NoError(err)
	require.Equal(peers, result)
}
