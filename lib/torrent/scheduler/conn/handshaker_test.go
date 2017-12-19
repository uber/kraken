package conn

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/lib/torrent/storage"
)

func TestHandshakerSetsConnFieldsProperly(t *testing.T) {
	require := require.New(t)

	config := ConfigFixture()

	h1 := HandshakerFixture(config)
	l1, err := net.Listen("tcp", "localhost:0")
	require.NoError(err)
	defer l1.Close()

	h2 := HandshakerFixture(config)

	info, cleanup := storage.TorrentInfoFixture(4, 1)
	defer cleanup()

	var wg sync.WaitGroup

	start := time.Now()

	wg.Add(1)
	go func() {
		defer wg.Done()

		nc, err := l1.Accept()
		require.NoError(err)

		pc, err := h1.Accept(nc)
		require.NoError(err)
		require.Equal(h2.peerID, pc.PeerID())
		require.Equal(info.Name(), pc.Name())
		require.Equal(info.InfoHash(), pc.InfoHash())
		require.Equal(info.Bitfield(), pc.Bitfield())

		c, err := h1.Establish(pc, info)
		require.NoError(err)
		require.Equal(h2.peerID, c.PeerID())
		require.Equal(info.InfoHash(), c.InfoHash())
		require.True(c.CreatedAt().After(start))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		c, b, err := h2.Initialize(h1.peerID, l1.Addr().String(), info)
		require.NoError(err)
		require.Equal(h1.peerID, c.PeerID())
		require.Equal(info.InfoHash(), c.InfoHash())
		require.True(c.CreatedAt().After(start))
		require.Equal(info.Bitfield(), b)
	}()

	wg.Wait()
}
