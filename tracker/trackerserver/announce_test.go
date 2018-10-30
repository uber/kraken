package trackerserver

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/tracker/announceclient"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func newAnnounceClient(pctx core.PeerContext, addr string) announceclient.Client {
	return announceclient.New(pctx, healthcheck.NoopFailed(hostlist.Fixture(addr)), nil)
}

func TestAnnounceSinglePeerResponse(t *testing.T) {
	for _, version := range []int{announceclient.V1, announceclient.V2} {
		t.Run(fmt.Sprintf("V%d", version), func(t *testing.T) {
			require := require.New(t)

			config := Config{AnnounceInterval: 5 * time.Second}

			mocks, cleanup := newServerMocks(t, config)
			defer cleanup()

			addr, stop := testutil.StartServer(mocks.handler())
			defer stop()

			blob := core.NewBlobFixture()
			pctx := core.PeerContextFixture()

			client := newAnnounceClient(pctx, addr)

			peers := []*core.PeerInfo{core.PeerInfoFixture()}

			mocks.originStore.EXPECT().GetOrigins(blob.Digest).Return(nil, nil)
			mocks.peerStore.EXPECT().GetPeers(
				blob.MetaInfo.InfoHash(), gomock.Any()).Return(peers, nil)
			mocks.peerStore.EXPECT().UpdatePeer(
				blob.MetaInfo.InfoHash(), core.PeerInfoFromContext(pctx, false)).Return(nil)

			result, interval, err := client.Announce(
				blob.Digest, blob.MetaInfo.InfoHash(), false, version)
			require.NoError(err)
			require.Equal(peers, result)
			require.Equal(config.AnnounceInterval, interval)
		})
	}
}

func TestAnnounceUnavailablePeerStoreCanStillProvideOrigins(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	pctx := core.PeerContextFixture()
	blob := core.NewBlobFixture()
	origins := []*core.PeerInfo{core.OriginPeerInfoFixture()}

	client := newAnnounceClient(pctx, addr)

	storeErr := errors.New("some storage error")

	mocks.peerStore.EXPECT().UpdatePeer(
		blob.MetaInfo.InfoHash(), core.PeerInfoFromContext(pctx, false)).Return(storeErr)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash(), gomock.Any()).Return(nil, storeErr)
	mocks.originStore.EXPECT().GetOrigins(blob.Digest).Return(origins, nil)

	result, _, err := client.Announce(
		blob.Digest, blob.MetaInfo.InfoHash(), false, announceclient.V2)
	require.NoError(err)
	require.Equal(origins, result)
}

func TestAnnouceUnavailableOriginClusterCanStillProvidePeers(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newServerMocks(t, Config{})
	defer cleanup()

	addr, stop := testutil.StartServer(mocks.handler())
	defer stop()

	pctx := core.PeerContextFixture()
	blob := core.NewBlobFixture()

	client := newAnnounceClient(pctx, addr)

	peers := []*core.PeerInfo{core.PeerInfoFixture()}

	mocks.peerStore.EXPECT().UpdatePeer(
		blob.MetaInfo.InfoHash(), core.PeerInfoFromContext(pctx, false)).Return(nil)
	mocks.peerStore.EXPECT().GetPeers(
		blob.MetaInfo.InfoHash(), gomock.Any()).Return(peers, nil)
	mocks.originStore.EXPECT().GetOrigins(blob.Digest).Return(nil, errors.New("some error"))

	result, _, err := client.Announce(
		blob.Digest, blob.MetaInfo.InfoHash(), false, announceclient.V2)
	require.NoError(err)
	require.Equal(peers, result)
}

func TestAnnounceRequestGetDigestBackwardsCompatibility(t *testing.T) {
	d := core.DigestFixture()
	h := core.InfoHashFixture()
	p := core.PeerInfoFixture()

	tests := []struct {
		desc    string
		request *announceclient.Request
	}{
		{
			"name only (old version)",
			&announceclient.Request{
				Name:     d.Hex(),
				InfoHash: h,
				Peer:     p,
			},
		}, {
			"digest and name (current version)",
			&announceclient.Request{
				Name:     d.Hex(),
				Digest:   &d,
				InfoHash: h,
				Peer:     p,
			},
		}, {
			"digest only (future version)",
			&announceclient.Request{
				Digest:   &d,
				InfoHash: h,
				Peer:     p,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			result, err := test.request.GetDigest()
			require.NoError(err)
			require.Equal(d, result)
		})
	}
}
