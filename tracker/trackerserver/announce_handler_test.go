package trackerserver

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/peercontext"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/storage"

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
	t.Run("Return 200 and single peer response", func(t *testing.T) {
		require := require.New(t)

		announceRequest, _ := http.NewRequest("GET", announceRequestPath, nil)
		mocks := &testMocks{}
		defer mocks.mockController(t)()

		peers := []*torlib.PeerInfo{{
			InfoHash: peer.InfoHash,
			PeerID:   peer.PeerID,
			IP:       peer.IP,
			Port:     peer.Port,
		}}

		mocks.datastore.EXPECT().GetOrigins(mi.InfoHash.HexString()).Return(nil, nil)
		mocks.datastore.EXPECT().GetPeers(mi.InfoHash.HexString()).Return(peers, nil)
		mocks.datastore.EXPECT().UpdatePeer(peer).Return(nil)
		response := mocks.CreateHandlerAndServeRequest(announceRequest)
		requireStatus(t, response, 200)
		ar := torlib.AnnouncerResponse{}
		require.NoError(json.NewDecoder(response.Body).Decode(&ar))
		require.Equal(ar.Peers, peers)
	})
	t.Run("Return origins", func(t *testing.T) {
		require := require.New(t)

		mocks := new(testMocks)
		defer mocks.mockController(t)()

		req, err := http.NewRequest("GET", announceRequestPath, nil)
		require.NoError(err)

		origins := []*torlib.PeerInfo{{
			InfoHash: peer.InfoHash,
			PeerID:   peer.PeerID,
			IP:       peer.IP,
			Port:     peer.Port,
			Origin:   true,
		}}

		mocks.datastore.EXPECT().GetOrigins(mi.InfoHash.HexString()).Return(origins, nil)
		mocks.datastore.EXPECT().GetPeers(mi.InfoHash.HexString()).Return(nil, nil)
		mocks.datastore.EXPECT().UpdatePeer(peer).Return(nil)

		resp := mocks.CreateHandlerAndServeRequest(req)
		requireStatus(t, resp, 200)
		ar := torlib.AnnouncerResponse{}
		require.NoError(json.NewDecoder(resp.Body).Decode(&ar))
		require.Equal(origins, ar.Peers)
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
		origins := []*torlib.PeerInfo{{
			InfoHash: infoHash,
			PeerID:   originPCtx.PeerID.String(),
			IP:       originPCtx.IP,
			Port:     int64(originPCtx.Port),
			DC:       originPCtx.Zone,
			Origin:   true,
			Complete: true,
		}}

		mocks.originCluster.EXPECT().Owners(digest).Return([]peercontext.PeerContext{originPCtx}, nil)

		mocks.datastore.EXPECT().UpdatePeer(peer).Return(nil)
		mocks.datastore.EXPECT().GetPeers(infoHash).Return(nil, nil)
		mocks.datastore.EXPECT().GetOrigins(infoHash).Return(nil, storage.ErrNoOrigins)
		mocks.datastore.EXPECT().UpdateOrigins(infoHash, origins).Return(nil)

		resp := mocks.CreateHandlerAndServeRequest(req)
		requireStatus(t, resp, 200)
		ar := torlib.AnnouncerResponse{}
		require.NoError(json.NewDecoder(resp.Body).Decode(&ar))
		require.Equal(origins, ar.Peers)
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
		origins := []*torlib.PeerInfo{{
			InfoHash: infoHash,
			PeerID:   originPCtx.PeerID.String(),
			IP:       originPCtx.IP,
			Port:     int64(originPCtx.Port),
			DC:       originPCtx.Zone,
			Origin:   true,
			Complete: true,
		}}

		mocks.originCluster.EXPECT().Owners(digest).Return([]peercontext.PeerContext{originPCtx}, nil)

		storageErr := errors.New("some storage error")

		mocks.datastore.EXPECT().UpdatePeer(peer).Return(storageErr)
		mocks.datastore.EXPECT().GetPeers(infoHash).Return(nil, storageErr)
		mocks.datastore.EXPECT().GetOrigins(infoHash).Return(nil, storageErr)

		resp := mocks.CreateHandlerAndServeRequest(req)
		requireStatus(t, resp, 200)
		ar := torlib.AnnouncerResponse{}
		require.NoError(json.NewDecoder(resp.Body).Decode(&ar))
		require.Equal(origins, ar.Peers)
	})
	t.Run("No origins and unavailable origin cluster can still provide peers", func(t *testing.T) {
		require := require.New(t)

		mocks := new(testMocks)
		defer mocks.mockController(t)()

		req, err := http.NewRequest("GET", announceRequestPath, nil)
		require.NoError(err)

		infoHash := mi.InfoHash.HexString()
		digest := image.NewSHA256DigestFromHex(mi.Info.Name)

		otherPeers := []*torlib.PeerInfo{{
			InfoHash: peer.InfoHash,
			PeerID:   peer.PeerID,
			IP:       peer.IP,
			Port:     peer.Port,
		}}

		mocks.originCluster.EXPECT().Owners(digest).Return(nil, errors.New("origin cluster error"))

		mocks.datastore.EXPECT().UpdatePeer(peer).Return(nil)
		mocks.datastore.EXPECT().GetPeers(infoHash).Return(otherPeers, nil)
		mocks.datastore.EXPECT().GetOrigins(infoHash).Return(nil, storage.ErrNoOrigins)

		resp := mocks.CreateHandlerAndServeRequest(req)
		requireStatus(t, resp, 200)
		ar := torlib.AnnouncerResponse{}
		require.NoError(json.NewDecoder(resp.Body).Decode(&ar))
		require.Equal(otherPeers, ar.Peers)
	})
}

// requireStatus fails if the response is not of the given status. Logs the body
// of the response on failure for debugging purposes.
func requireStatus(t *testing.T, r *http.Response, status int) {
	if r.StatusCode != status {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatalf(
				"Expected status %d, got %d. Could not read body: %v",
				status, r.StatusCode, err)
		}
		t.Fatalf(
			"Expected status %d, got %d. Body: %s",
			status, r.StatusCode, string(b))
	}
}
