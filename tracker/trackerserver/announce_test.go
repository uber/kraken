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

	pctx := core.PeerContextFixture()
	mi := core.MetaInfoFixture()
	peer := core.ToPeerInfoFixture(pctx, mi)

	client := announceclient.New(pctx, serverset.MustRoundRobin(addr))

	peers := []*core.PeerInfo{core.PeerInfoForMetaInfoFixture(mi)}

	mocks.peerStore.EXPECT().GetOrigins(mi.InfoHash.HexString()).Return(nil, nil)
	mocks.peerStore.EXPECT().GetPeers(mi.InfoHash.HexString(), gomock.Any()).Return(peers, nil)
	mocks.peerStore.EXPECT().UpdatePeer(peer).Return(nil)

	result, err := client.Announce(mi.Name(), mi.InfoHash, false)
	require.NoError(err)
	require.Equal(peers, result)
}

func TestAnnounceReturnsCachedOrigin(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t)
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	pctx := core.PeerContextFixture()
	octx := core.OriginContextFixture()
	mi := core.MetaInfoFixture()
	peer := core.ToPeerInfoFixture(pctx, mi)

	client := announceclient.New(pctx, serverset.MustRoundRobin(addr))

	origins := []*core.PeerInfo{core.ToPeerInfoFixture(octx, mi)}

	mocks.peerStore.EXPECT().GetOrigins(mi.InfoHash.HexString()).Return(origins, nil)
	mocks.peerStore.EXPECT().GetPeers(mi.InfoHash.HexString(), gomock.Any()).Return(nil, nil)
	mocks.peerStore.EXPECT().UpdatePeer(peer).Return(nil)

	result, err := client.Announce(mi.Name(), mi.InfoHash, false)
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
	mi := core.MetaInfoFixture()
	peer := core.ToPeerInfoFixture(pctx, mi)

	client := announceclient.New(pctx, serverset.MustRoundRobin(addr))

	origins := []*core.PeerInfo{core.ToPeerInfoFixture(octx, mi)}

	mocks.originCluster.EXPECT().Owners(
		core.NewSHA256DigestFromHex(mi.Info.Name)).Return([]core.PeerContext{octx}, nil)

	mocks.peerStore.EXPECT().UpdatePeer(peer).Return(nil)
	mocks.peerStore.EXPECT().GetPeers(mi.InfoHash.HexString(), gomock.Any()).Return(nil, nil)
	mocks.peerStore.EXPECT().GetOrigins(mi.InfoHash.HexString()).Return(nil, storage.ErrNoOrigins)
	mocks.peerStore.EXPECT().UpdateOrigins(mi.InfoHash.HexString(), origins).Return(nil)

	result, err := client.Announce(mi.Name(), mi.InfoHash, false)
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
	mi := core.MetaInfoFixture()
	peer := core.ToPeerInfoFixture(pctx, mi)

	client := announceclient.New(pctx, serverset.MustRoundRobin(addr))

	origins := []*core.PeerInfo{core.ToPeerInfoFixture(octx, mi)}

	mocks.originCluster.EXPECT().Owners(
		core.NewSHA256DigestFromHex(mi.Info.Name)).Return([]core.PeerContext{octx}, nil)

	storageErr := errors.New("some storage error")

	mocks.peerStore.EXPECT().UpdatePeer(peer).Return(storageErr)
	mocks.peerStore.EXPECT().GetPeers(mi.InfoHash.HexString(), gomock.Any()).Return(nil, storageErr)
	mocks.peerStore.EXPECT().GetOrigins(mi.InfoHash.HexString()).Return(nil, storageErr)

	result, err := client.Announce(mi.Name(), mi.InfoHash, false)
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
	mi := core.MetaInfoFixture()
	peer := core.ToPeerInfoFixture(pctx, mi)

	client := announceclient.New(pctx, serverset.MustRoundRobin(addr))

	peers := []*core.PeerInfo{core.PeerInfoForMetaInfoFixture(mi)}

	mocks.originCluster.EXPECT().Owners(
		core.NewSHA256DigestFromHex(mi.Name())).Return(nil, errors.New("origin cluster error"))

	mocks.peerStore.EXPECT().UpdatePeer(peer).Return(nil)
	mocks.peerStore.EXPECT().GetPeers(mi.InfoHash.HexString(), gomock.Any()).Return(peers, nil)
	mocks.peerStore.EXPECT().GetOrigins(mi.InfoHash.HexString()).Return(nil, storage.ErrNoOrigins)

	result, err := client.Announce(mi.Name(), mi.InfoHash, false)
	require.NoError(err)
	require.Equal(peers, result)
}
