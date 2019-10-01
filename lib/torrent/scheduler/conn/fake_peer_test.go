package conn

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/lib/torrent/storage"
)

func TestFakePeer(t *testing.T) {
	require := require.New(t)

	p, err := NewFakePeer()
	require.NoError(err)
	defer p.Close()

	h := HandshakerFixture(ConfigFixture())

	info := storage.TorrentInfoFixture(32, 4)

	res, err := h.Initialize(p.PeerID(), p.Addr(), info, nil, "noexist")
	require.NoError(err)

	require.Equal(p.PeerID(), res.Conn.PeerID())
	require.Equal(info.InfoHash(), res.Conn.InfoHash())
	require.Equal(info.Bitfield().Len(), res.Bitfield.Len())
	require.False(res.Bitfield.Any())
}
