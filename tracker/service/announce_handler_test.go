package service

import (
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/testutils"
	"code.uber.internal/infra/kraken/torlib"

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

	t.Run("Return 500 if missing parameters", func(t *testing.T) {
		announceRequest, _ := http.NewRequest("GET", "/announce", nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		response := mocks.CreateHandlerAndServeRequest(announceRequest)
		require.Equal(t, 500, response.StatusCode)
	})
	t.Run("Return 200 and empty bencoded response", func(t *testing.T) {

		announceRequest, _ := http.NewRequest("GET", announceRequestPath, nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().GetPeers(mi.InfoHash.HexString()).Return([]*torlib.PeerInfo{}, nil)
		mocks.datastore.EXPECT().UpdatePeer(peer).Return(nil)
		response := mocks.CreateHandlerAndServeRequest(announceRequest)
		require.Equal(t, 200, response.StatusCode)
		announceResponse := torlib.AnnouncerResponse{}
		bencode.Unmarshal(response.Body, &announceResponse)
		assert.Equal(t, announceResponse.Interval, int64(0))
		assert.Equal(t, announceResponse.Peers, []torlib.PeerInfo{})
	})
	t.Run("Return 200 and single peer bencoded response", func(t *testing.T) {

		announceRequest, _ := http.NewRequest("GET", announceRequestPath, nil)
		mocks := &testMocks{}
		defer mocks.mockController(t)()

		peerTo := &torlib.PeerInfo{
			InfoHash:        peer.InfoHash,
			PeerID:          peer.PeerID,
			IP:              peer.IP,
			Port:            peer.Port,
			DC:              peer.DC,
			BytesDownloaded: peer.BytesDownloaded,
			BytesLeft:       peer.BytesLeft,
			BytesUploaded:   peer.BytesUploaded,
			Event:           peer.Event,
		}

		mocks.datastore.EXPECT().UpdatePeer(peer).Return(nil)
		mocks.datastore.EXPECT().GetPeers(mi.InfoHash.HexString()).Return([]*torlib.PeerInfo{peer}, nil)
		response := mocks.CreateHandlerAndServeRequest(announceRequest)
		testutils.RequireStatus(t, response, 200)
		announceResponse := torlib.AnnouncerResponse{}
		bencode.Unmarshal(response.Body, &announceResponse)
		assert.Equal(t, announceResponse.Interval, int64(0))
		assert.Equal(t, announceResponse.Peers, []torlib.PeerInfo{*peerTo})
	})
}
