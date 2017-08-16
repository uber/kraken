package service

import (
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/torlib"

	bencode "github.com/jackpal/bencode-go"
	"github.com/stretchr/testify/require"
)

func getPeerIDs(peers []torlib.PeerInfo) []string {
	s := make([]string, len(peers))
	for i, p := range peers {
		s[i] = p.PeerID
	}
	return s
}

func TestTestAnnouncer(t *testing.T) {
	require := require.New(t)

	addr, stop := TestAnnouncer()
	defer stop()

	str := `d8:announce10:trackerurl4:infod6:lengthi2e4:name8:torrent012:piece lengthi1e6:pieces0:eePASS`
	mi, err := torlib.NewMetaInfoFromBytes([]byte(str))
	require.NoError(err)
	p1 := &torlib.PeerInfo{
		PeerID: "peer1",
	}
	p2 := &torlib.PeerInfo{
		PeerID: "peer2",
	}

	// Announcing p1 should return p1.

	resp, err := http.Get("http://" + addr + createAnnouncePath(mi, p1))
	require.NoError(err)

	ar := torlib.AnnouncerResponse{}
	err = bencode.Unmarshal(resp.Body, &ar)
	require.NoError(err)

	require.Equal([]string{p1.PeerID}, getPeerIDs(ar.Peers))

	// Announce p2 should return both p1 and p2.

	resp, err = http.Get("http://" + addr + createAnnouncePath(mi, p2))
	require.NoError(err)

	ar = torlib.AnnouncerResponse{}
	err = bencode.Unmarshal(resp.Body, &ar)
	require.NoError(err)

	require.Equal([]string{p1.PeerID, p2.PeerID}, getPeerIDs(ar.Peers))
}
