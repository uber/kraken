package conn

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/utils/bitsetutil"
)

func TestHandshakerSetsConnFieldsProperly(t *testing.T) {
	require := require.New(t)

	config := ConfigFixture()

	h1 := HandshakerFixture(config)
	l1, err := net.Listen("tcp", "localhost:0")
	require.NoError(err)
	defer l1.Close()

	namespace := core.TagFixture()
	h2 := HandshakerFixture(config)

	info := storage.TorrentInfoFixture(4, 1)
	emptyRemoteBitfields := make(RemoteBitfields)
	remoteBitfields := RemoteBitfields{
		core.PeerIDFixture(): bitsetutil.FromBools(true, false),
		core.PeerIDFixture(): bitsetutil.FromBools(false, true),
	}

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
		require.Equal(info.Digest(), pc.Digest())
		require.Equal(info.InfoHash(), pc.InfoHash())
		require.Equal(info.Bitfield(), pc.Bitfield())
		require.Equal(namespace, pc.Namespace())

		c, err := h1.Establish(pc, info, remoteBitfields)
		require.NoError(err)
		require.Equal(h2.peerID, c.PeerID())
		require.Equal(info.InfoHash(), c.InfoHash())
		require.True(c.CreatedAt().After(start))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		r, err := h2.Initialize(h1.peerID, l1.Addr().String(), info, emptyRemoteBitfields, namespace)
		require.NoError(err)
		require.Equal(h1.peerID, r.Conn.PeerID())
		require.Equal(info.InfoHash(), r.Conn.InfoHash())
		require.True(r.Conn.CreatedAt().After(start))
		require.Equal(info.Bitfield(), r.Bitfield)
		require.Equal(remoteBitfields, r.RemoteBitfields)
	}()

	wg.Wait()
}
