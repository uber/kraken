package service

import (
	"errors"
	"net/http"
	"sort"
	"testing"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/mocks/origin/blobclient"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/testutil"

	"github.com/golang/mock/gomock"
	bencode "github.com/jackpal/bencode-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnnounceEndPoint(t *testing.T) {
	mi := torlib.MetaInfoFixture()
	peer := &torlib.PeerInfo{
		InfoHash: mi.InfoHash.HexString(),
		PeerID:   "peer",
		IP:       "127.0.0.1",
		Port:     8080,
		DC:       "sjc1",
	}
	announceRequestPath := createAnnouncePath(mi, peer)

	t.Run("Return 400 if missing parameters", func(t *testing.T) {
		announceRequest, _ := http.NewRequest("GET", "/announce", nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		response := mocks.CreateHandlerAndServeRequest(announceRequest)
		require.Equal(t, 400, response.StatusCode)
	})
	t.Run("Return 404 if no peers found", func(t *testing.T) {

		announceRequest, _ := http.NewRequest("GET", announceRequestPath, nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().GetOrigins(mi.InfoHash.HexString()).Return(nil, nil)
		mocks.datastore.EXPECT().GetPeers(mi.InfoHash.HexString()).Return(nil, nil)
		mocks.datastore.EXPECT().UpdatePeer(peer).Return(nil)
		response := mocks.CreateHandlerAndServeRequest(announceRequest)
		require.Equal(t, 404, response.StatusCode)
	})
	t.Run("Return 200 and single peer bencoded response", func(t *testing.T) {

		announceRequest, _ := http.NewRequest("GET", announceRequestPath, nil)
		mocks := &testMocks{}
		defer mocks.mockController(t)()

		peerTo := &torlib.PeerInfo{
			InfoHash: peer.InfoHash,
			PeerID:   peer.PeerID,
			IP:       peer.IP,
			Port:     peer.Port,
		}

		mocks.datastore.EXPECT().GetOrigins(mi.InfoHash.HexString()).Return(nil, nil)
		mocks.datastore.EXPECT().GetPeers(mi.InfoHash.HexString()).Return([]*torlib.PeerInfo{peerTo}, nil)
		mocks.datastore.EXPECT().UpdatePeer(peer).Return(nil)
		response := mocks.CreateHandlerAndServeRequest(announceRequest)
		testutil.RequireStatus(t, response, 200)
		announceResponse := torlib.AnnouncerResponse{}
		bencode.Unmarshal(response.Body, &announceResponse)
		assert.Equal(t, announceResponse.Interval, int64(5))
		assert.Equal(t, announceResponse.Peers, []torlib.PeerInfo{*peerTo})
	})
	t.Run("Return origins", func(t *testing.T) {
		require := require.New(t)

		mocks := new(testMocks)
		defer mocks.mockController(t)()

		req, err := http.NewRequest("GET", announceRequestPath, nil)
		require.NoError(err)

		origin := &torlib.PeerInfo{
			InfoHash: peer.InfoHash,
			PeerID:   peer.PeerID,
			IP:       peer.IP,
			Port:     peer.Port,
			Origin:   true,
		}

		mocks.datastore.EXPECT().GetOrigins(mi.InfoHash.HexString()).Return([]*torlib.PeerInfo{origin}, nil)
		mocks.datastore.EXPECT().GetPeers(mi.InfoHash.HexString()).Return(nil, nil)
		mocks.datastore.EXPECT().UpdatePeer(peer).Return(nil)

		resp := mocks.CreateHandlerAndServeRequest(req)
		testutil.RequireStatus(t, resp, 200)
		ar := torlib.AnnouncerResponse{}
		bencode.Unmarshal(resp.Body, &ar)
		origin.Origin = false
		require.Equal([]torlib.PeerInfo{*origin}, ar.Peers)
	})
	t.Run("No origins in storage makes requests to origin cluster and upserts origins", func(t *testing.T) {
		require := require.New(t)

		mocks := new(testMocks)
		defer mocks.mockController(t)()

		req, err := http.NewRequest("GET", announceRequestPath, nil)
		require.NoError(err)

		infoHash := mi.InfoHash.HexString()
		digest := image.NewSHA256DigestFromHex(mi.Info.Name)
		originPCtx := peercontext.Fixture()
		origin := &torlib.PeerInfo{
			InfoHash: infoHash,
			PeerID:   originPCtx.PeerID.String(),
			IP:       originPCtx.IP,
			Port:     int64(originPCtx.Port),
			DC:       originPCtx.Zone,
			Origin:   true,
			Complete: true,
		}

		mockBlobClient := mockblobclient.NewMockClient(mocks.ctrl)

		mocks.originResolver.EXPECT().Resolve(digest).Return([]blobclient.Client{mockBlobClient}, nil)
		mockBlobClient.EXPECT().GetPeerContext().Return(originPCtx, nil)

		mocks.datastore.EXPECT().UpdatePeer(peer).Return(nil)
		mocks.datastore.EXPECT().GetPeers(infoHash).Return(nil, nil)
		mocks.datastore.EXPECT().GetOrigins(infoHash).Return(nil, storage.ErrNoOrigins)
		mocks.datastore.EXPECT().UpdateOrigins(infoHash, []*torlib.PeerInfo{origin}).Return(nil)

		resp := mocks.CreateHandlerAndServeRequest(req)
		testutil.RequireStatus(t, resp, 200)
		ar := torlib.AnnouncerResponse{}
		bencode.Unmarshal(resp.Body, &ar)
		origin.Origin = false
		origin.Complete = false
		require.Equal([]torlib.PeerInfo{*origin}, ar.Peers)
	})
	t.Run("Unavailable peer store can still provide origin peers", func(t *testing.T) {
		require := require.New(t)

		mocks := new(testMocks)
		defer mocks.mockController(t)()

		req, err := http.NewRequest("GET", announceRequestPath, nil)
		require.NoError(err)

		infoHash := mi.InfoHash.HexString()
		digest := image.NewSHA256DigestFromHex(mi.Info.Name)
		originPCtx := peercontext.Fixture()
		origin := &torlib.PeerInfo{
			InfoHash: infoHash,
			PeerID:   originPCtx.PeerID.String(),
			IP:       originPCtx.IP,
			Port:     int64(originPCtx.Port),
			DC:       originPCtx.Zone,
			Origin:   true,
		}

		mockBlobClient := mockblobclient.NewMockClient(mocks.ctrl)

		mocks.originResolver.EXPECT().Resolve(digest).Return([]blobclient.Client{mockBlobClient}, nil)
		mockBlobClient.EXPECT().GetPeerContext().Return(originPCtx, nil)

		storageErr := errors.New("some storage error")

		mocks.datastore.EXPECT().UpdatePeer(peer).Return(storageErr)
		mocks.datastore.EXPECT().GetPeers(infoHash).Return(nil, storageErr)
		mocks.datastore.EXPECT().GetOrigins(infoHash).Return(nil, storageErr)

		resp := mocks.CreateHandlerAndServeRequest(req)
		testutil.RequireStatus(t, resp, 200)
		ar := torlib.AnnouncerResponse{}
		bencode.Unmarshal(resp.Body, &ar)
		origin.Origin = false
		require.Equal([]torlib.PeerInfo{*origin}, ar.Peers)
	})
	t.Run("No origins and unavailable origin cluster can still provide peers", func(t *testing.T) {
		require := require.New(t)

		mocks := new(testMocks)
		defer mocks.mockController(t)()

		req, err := http.NewRequest("GET", announceRequestPath, nil)
		require.NoError(err)

		infoHash := mi.InfoHash.HexString()
		digest := image.NewSHA256DigestFromHex(mi.Info.Name)

		otherPeer := &torlib.PeerInfo{
			InfoHash: peer.InfoHash,
			PeerID:   peer.PeerID,
			IP:       peer.IP,
			Port:     peer.Port,
		}

		mocks.originResolver.EXPECT().Resolve(digest).Return(nil, errors.New("origin cluster error"))

		mocks.datastore.EXPECT().UpdatePeer(peer).Return(nil)
		mocks.datastore.EXPECT().GetPeers(infoHash).Return([]*torlib.PeerInfo{otherPeer}, nil)
		mocks.datastore.EXPECT().GetOrigins(infoHash).Return(nil, storage.ErrNoOrigins)

		resp := mocks.CreateHandlerAndServeRequest(req)
		testutil.RequireStatus(t, resp, 200)
		ar := torlib.AnnouncerResponse{}
		bencode.Unmarshal(resp.Body, &ar)
		require.Equal([]torlib.PeerInfo{*otherPeer}, ar.Peers)
	})
}

func TestAnnounceHandlerRequestOriginsConcurrency(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mi := torlib.MetaInfoFixture()
	infoHash := mi.InfoHash.HexString()
	digest := image.NewSHA256DigestFromHex(mi.Info.Name)

	mockOriginResolver := mockblobclient.NewMockClusterResolver(ctrl)

	var clients []blobclient.Client
	var peerIDs []string
	for i := 0; i < 3; i++ {
		mockClient := mockblobclient.NewMockClient(ctrl)
		pctx := peercontext.Fixture()
		mockClient.EXPECT().GetPeerContext().Return(pctx, nil)

		peerIDs = append(peerIDs, pctx.PeerID.String())
		clients = append(clients, mockClient)
	}
	sort.Strings(peerIDs)

	mockOriginResolver.EXPECT().Resolve(digest).Return(clients, nil)

	h := &announceHandler{originResolver: mockOriginResolver}

	origins, err := h.requestOrigins(infoHash, mi.Info.Name)
	require.NoError(err)
	require.Equal(peerIDs, torlib.SortedPeerIDs(origins))
}

func TestAnnounceHandlerRequestOriginsPartialErrors(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mi := torlib.MetaInfoFixture()
	infoHash := mi.InfoHash.HexString()
	digest := image.NewSHA256DigestFromHex(mi.Info.Name)

	mockOriginResolver := mockblobclient.NewMockClusterResolver(ctrl)

	mockClient1 := mockblobclient.NewMockClient(ctrl)
	mockClient2 := mockblobclient.NewMockClient(ctrl)
	mockClient3 := mockblobclient.NewMockClient(ctrl)

	pctx := peercontext.Fixture()

	mockClient1.EXPECT().GetPeerContext().Return(pctx, nil)
	mockClient2.EXPECT().GetPeerContext().Return(peercontext.PeerContext{}, errors.New("some error"))
	mockClient3.EXPECT().GetPeerContext().Return(peercontext.PeerContext{}, errors.New("some error"))

	mockOriginResolver.EXPECT().Resolve(digest).Return(
		[]blobclient.Client{mockClient1, mockClient2, mockClient3}, nil)

	h := &announceHandler{originResolver: mockOriginResolver}

	origins, err := h.requestOrigins(infoHash, mi.Info.Name)
	require.Error(err)
	require.Len(err.(errutil.MultiError), 2)
	require.Equal([]string{pctx.PeerID.String()}, torlib.SortedPeerIDs(origins))
}
