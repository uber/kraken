package service

import (
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/tracker/storage"
	bencode "github.com/jackpal/bencode-go"
	"github.com/stretchr/testify/require"
)

func getPeerIDs(peers []storage.PeerInfo) []string {
	s := make([]string, len(peers))
	for i, p := range peers {
		s[i] = p.PeerID
	}
	return s
}

func TestTestAnnouncer(t *testing.T) {
	require := require.New(t)

	addr := "localhost:26232"
	stop := TestAnnouncer(addr)
	defer stop()

	tor := storage.TorrentFixture()
	p1 := storage.PeerForTorrentFixture(tor)
	p2 := storage.PeerForTorrentFixture(tor)

	// Announcing p1 should return p1.

	resp, err := http.Get("http://" + addr + createAnnouncePath(tor, p1))
	require.NoError(err)

	ar := AnnouncerResponse{}
	err = bencode.Unmarshal(resp.Body, &ar)
	require.NoError(err)

	require.Equal([]string{p1.PeerID}, getPeerIDs(ar.Peers))

	// Announce p2 should return both p1 and p2.

	resp, err = http.Get("http://" + addr + createAnnouncePath(tor, p2))
	require.NoError(err)

	ar = AnnouncerResponse{}
	err = bencode.Unmarshal(resp.Body, &ar)
	require.NoError(err)

	require.Equal([]string{p1.PeerID, p2.PeerID}, getPeerIDs(ar.Peers))
}
