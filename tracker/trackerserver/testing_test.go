package trackerserver

import (
	"encoding/json"
	"net/http"
	"testing"

	"code.uber.internal/infra/kraken/core"

	"github.com/stretchr/testify/require"
)

func TestTestAnnouncer(t *testing.T) {
	require := require.New(t)

	addr, stop := TestAnnouncer()
	defer stop()

	mi := core.MetaInfoFixture()
	p1 := &core.PeerInfo{
		PeerID:   "peer1",
		Complete: false,
	}
	p2 := &core.PeerInfo{
		PeerID:   "peer2",
		Complete: false,
	}

	// Announcing p1 should return p1.

	resp, err := http.Get("http://" + addr + createAnnouncePath(mi, p1))
	require.NoError(err)

	ar := core.AnnouncerResponse{}
	require.NoError(json.NewDecoder(resp.Body).Decode(&ar))

	require.Equal([]string{p1.PeerID}, core.SortedPeerIDs(ar.Peers))

	// Announce p2 should return both p1 and p2.

	resp, err = http.Get("http://" + addr + createAnnouncePath(mi, p2))
	require.NoError(err)

	ar = core.AnnouncerResponse{}
	require.NoError(json.NewDecoder(resp.Body).Decode(&ar))

	require.Equal([]string{p1.PeerID, p2.PeerID}, core.SortedPeerIDs(ar.Peers))
}
