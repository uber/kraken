package trackerserver

import (
	"errors"
	"testing"

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

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	blob := core.NewBlobFixture()
	pctx := core.PeerContextFixture()

	client := announceclient.New(pctx, serverset.MustRoundRobin(addr))

	peers := []*core.PeerInfo{blob.PeerInfo()}

	mocks.peerStore.EXPECT().GetOrigins(blob.MetaInfo.InfoHash.HexString()).Return(nil, nil)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash.HexString(), gomock.Any()).Return(peers, nil)
	mocks.peerStore.EXPECT().UpdatePeer(blob.PeerInfoFromContext(pctx)).Return(nil)

	result, err := client.Announce(blob.MetaInfo.Name(), blob.MetaInfo.InfoHash, false)
	require.NoError(err)
	require.Equal(peers, result)
}

func TestAnnounceReturnsCachedOrigin(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	blob := core.NewBlobFixture()
	pctx := core.PeerContextFixture()
	octx := core.OriginContextFixture()

	client := announceclient.New(pctx, serverset.MustRoundRobin(addr))

	origins := []*core.PeerInfo{blob.PeerInfoFromContext(octx)}

	mocks.peerStore.EXPECT().GetOrigins(blob.MetaInfo.InfoHash.HexString()).Return(origins, nil)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash.HexString(), gomock.Any()).Return(nil, nil)
	mocks.peerStore.EXPECT().UpdatePeer(blob.PeerInfoFromContext(pctx)).Return(nil)

	result, err := client.Announce(blob.MetaInfo.Name(), blob.MetaInfo.InfoHash, false)
	require.NoError(err)
	require.Equal(origins, result)
}

func TestAnnounceMissingOriginsFetchesAndCachesOrigins(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	pctx := core.PeerContextFixture()
	octx := core.OriginContextFixture()
	blob := core.NewBlobFixture()

	client := announceclient.New(pctx, serverset.MustRoundRobin(addr))

	origins := []*core.PeerInfo{blob.PeerInfoFromContext(octx)}

	mocks.originCluster.EXPECT().Owners(blob.Digest).Return([]core.PeerContext{octx}, nil)

	mocks.peerStore.EXPECT().UpdatePeer(blob.PeerInfoFromContext(pctx)).Return(nil)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash.HexString(), gomock.Any()).Return(nil, nil)
	mocks.peerStore.EXPECT().GetOrigins(
		blob.MetaInfo.InfoHash.HexString()).Return(nil, storage.ErrNoOrigins)
	mocks.peerStore.EXPECT().UpdateOrigins(blob.MetaInfo.InfoHash.HexString(), origins).Return(nil)

	result, err := client.Announce(blob.MetaInfo.Name(), blob.MetaInfo.InfoHash, false)
	require.NoError(err)
	require.Equal(origins, result)
}

func TestAnnounceUnavailablePeerStoreCanStillProvideOrigins(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	pctx := core.PeerContextFixture()
	octx := core.OriginContextFixture()
	blob := core.NewBlobFixture()

	client := announceclient.New(pctx, serverset.MustRoundRobin(addr))

	origins := []*core.PeerInfo{blob.PeerInfoFromContext(octx)}

	mocks.originCluster.EXPECT().Owners(blob.Digest).Return([]core.PeerContext{octx}, nil)

	storageErr := errors.New("some storage error")

	mocks.peerStore.EXPECT().UpdatePeer(blob.PeerInfoFromContext(pctx)).Return(storageErr)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash.HexString(), gomock.Any()).Return(nil, storageErr)
	mocks.peerStore.EXPECT().GetOrigins(blob.MetaInfo.InfoHash.HexString()).Return(nil, storageErr)

	result, err := client.Announce(blob.MetaInfo.Name(), blob.MetaInfo.InfoHash, false)
	require.NoError(err)
	require.Equal(origins, result)
}

func TestAnnouceNoOriginsAndUnavailableOriginClusterCanStillProvidePeers(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	pctx := core.PeerContextFixture()
	blob := core.NewBlobFixture()

	client := announceclient.New(pctx, serverset.MustRoundRobin(addr))

	peers := []*core.PeerInfo{blob.PeerInfo()}

	mocks.originCluster.EXPECT().Owners(blob.Digest).Return(nil, errors.New("origin cluster error"))

	mocks.peerStore.EXPECT().UpdatePeer(blob.PeerInfoFromContext(pctx)).Return(nil)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash.HexString(), gomock.Any()).Return(peers, nil)
	mocks.peerStore.EXPECT().GetOrigins(
		blob.MetaInfo.InfoHash.HexString()).Return(nil, storage.ErrNoOrigins)

	result, err := client.Announce(blob.MetaInfo.Name(), blob.MetaInfo.InfoHash, false)
	require.NoError(err)
	require.Equal(peers, result)
}
