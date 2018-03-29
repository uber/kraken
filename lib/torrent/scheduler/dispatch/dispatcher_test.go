package dispatch

import (
	"errors"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/.gen/go/p2p"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/utils/memsize"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/willf/bitset"
)

type mockMessages struct {
	sent     []*conn.Message
	receiver chan *conn.Message
	closed   bool
}

func newMockMessages() *mockMessages {
	return &mockMessages{receiver: make(chan *conn.Message)}
}

func (m *mockMessages) Send(msg *conn.Message) error {
	if m.closed {
		return errors.New("messages closed")
	}
	m.sent = append(m.sent, msg)
	return nil
}

func (m *mockMessages) Receiver() <-chan *conn.Message { return m.receiver }

func (m *mockMessages) Close() {
	if m.closed {
		return
	}
	close(m.receiver)
	m.closed = true
}

func numRequestsPerPiece(messages Messages) map[int]int {
	requests := make(map[int]int)
	for _, msg := range messages.(*mockMessages).sent {
		if msg.Message.Type == p2p.Message_PIECE_REQUEST {
			requests[int(msg.Message.PieceRequest.Index)]++
		}
	}
	return requests
}

func announcedPieces(messages Messages) []int {
	var ps []int
	for _, msg := range messages.(*mockMessages).sent {
		if msg.Message.Type == p2p.Message_ANNOUCE_PIECE {
			ps = append(ps, int(msg.Message.AnnouncePiece.Index))
		}
	}
	return ps
}

func hasComplete(messages Messages) bool {
	for _, m := range messages.(*mockMessages).sent {
		if m.Message.Type == p2p.Message_COMPLETE {
			return true
		}
	}
	return false
}

func closed(messages Messages) bool {
	return messages.(*mockMessages).closed
}

type noopEvents struct{}

func (e noopEvents) DispatcherComplete(*Dispatcher) {}

func testDispatcher(config Config, clk clock.Clock, t storage.Torrent) *Dispatcher {
	return newDispatcher(
		config,
		tally.NoopScope,
		clk,
		networkevent.NewTestProducer(),
		noopEvents{},
		core.PeerIDFixture(),
		t)
}

func TestDispatcherSendUniquePieceRequestsWithinLimit(t *testing.T) {
	require := require.New(t)

	config := Config{
		PipelineLimit: 3,
	}
	clk := clock.NewMock()

	torrent, cleanup := storage.TorrentFixture(core.SizedBlobFixture(100, 1).MetaInfo)
	defer cleanup()

	d := testDispatcher(config, clk, torrent)

	var mu sync.Mutex
	var requestCount int
	totalRequestsPerPiece := make(map[int]int)
	totalRequestPerPeer := make(map[core.PeerID]int)

	// Add a bunch of peers concurrently which are saturated with pieces d needs.
	// We should send exactly <pipelineLimit> piece request per peer.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bitfield := bitset.New(uint(torrent.NumPieces())).Complement()
			p, err := d.addPeer(core.PeerIDFixture(), bitfield, newMockMessages())
			require.NoError(err)
			d.maybeRequestMorePieces(p)
			for i, n := range numRequestsPerPiece(p.messages) {
				require.True(n <= 1)
				mu.Lock()
				requestCount += n
				totalRequestsPerPiece[i] += n
				require.True(totalRequestsPerPiece[i] <= 1)
				totalRequestPerPeer[p.id] += n
				require.True(totalRequestPerPeer[p.id] <= config.PipelineLimit)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	require.Equal(config.PipelineLimit*10, requestCount)
}

func TestDispatcherResendFailedPieceRequests(t *testing.T) {
	require := require.New(t)

	config := Config{
		DisableEndgame: true,
	}
	clk := clock.NewMock()

	torrent, cleanup := storage.TorrentFixture(core.SizedBlobFixture(2, 1).MetaInfo)
	defer cleanup()

	d := testDispatcher(config, clk, torrent)

	// p1 has both pieces and sends requests for both.
	p1, err := d.addPeer(core.PeerIDFixture(), storage.BitSetFixture(true, true), newMockMessages())
	require.NoError(err)
	d.maybeRequestMorePieces(p1)
	require.Equal(map[int]int{
		0: 1,
		1: 1,
	}, numRequestsPerPiece(p1.messages))

	// p2 has piece 0 and sends no piece requests.
	p2, err := d.addPeer(
		core.PeerIDFixture(), storage.BitSetFixture(true, false), newMockMessages())
	require.NoError(err)
	d.maybeRequestMorePieces(p2)
	require.Equal(map[int]int{}, numRequestsPerPiece(p2.messages))

	// p3 has piece 1 and sends no piece requests.
	p3, err := d.addPeer(
		core.PeerIDFixture(), storage.BitSetFixture(false, true), newMockMessages())
	require.NoError(err)
	d.maybeRequestMorePieces(p3)
	require.Equal(map[int]int{}, numRequestsPerPiece(p3.messages))

	clk.Add(d.pieceRequestTimeout + 1)

	d.resendFailedPieceRequests()

	// p1 was not sent any new piece requests.
	require.Equal(map[int]int{
		0: 1,
		1: 1,
	}, numRequestsPerPiece(p1.messages))

	// p2 was sent a piece request for piece 0.
	require.Equal(map[int]int{
		0: 1,
	}, numRequestsPerPiece(p2.messages))

	// p3 was sent a piece request for piece 1.
	require.Equal(map[int]int{
		1: 1,
	}, numRequestsPerPiece(p3.messages))
}

func TestDispatcherSendErrorsMarksPieceRequestsUnsent(t *testing.T) {
	require := require.New(t)

	config := Config{
		DisableEndgame: true,
	}
	clk := clock.NewMock()

	torrent, cleanup := storage.TorrentFixture(core.SizedBlobFixture(1, 1).MetaInfo)
	defer cleanup()

	d := testDispatcher(config, clk, torrent)

	p1, err := d.addPeer(core.PeerIDFixture(), storage.BitSetFixture(true), newMockMessages())
	require.NoError(err)

	p1.messages.Close()

	// Send should fail since p1 messages are closed.
	d.maybeRequestMorePieces(p1)

	require.Equal(map[int]int{}, numRequestsPerPiece(p1.messages))

	p2, err := d.addPeer(core.PeerIDFixture(), storage.BitSetFixture(true), newMockMessages())
	require.NoError(err)

	// Send should succeed since pending requests were marked unsent.
	d.maybeRequestMorePieces(p2)

	require.Equal(map[int]int{
		0: 1,
	}, numRequestsPerPiece(p2.messages))
}

func TestDispatcherCalcPieceRequestTimeout(t *testing.T) {
	config := Config{
		PieceRequestMinTimeout:   5 * time.Second,
		PieceRequestTimeoutPerMb: 2 * time.Second,
	}

	tests := []struct {
		maxPieceLength uint64
		expected       time.Duration
	}{
		{512 * memsize.KB, 5 * time.Second},
		{memsize.MB, 5 * time.Second},
		{4 * memsize.MB, 8 * time.Second},
		{8 * memsize.MB, 16 * time.Second},
	}
	for _, test := range tests {
		t.Run(memsize.Format(test.maxPieceLength), func(t *testing.T) {
			timeout := config.calcPieceRequestTimeout(int64(test.maxPieceLength))
			require.Equal(t, test.expected, timeout)
		})
	}
}

func TestDispatcherEndgame(t *testing.T) {
	require := require.New(t)

	config := Config{
		PipelineLimit:    1,
		EndgameThreshold: 1,
	}
	clk := clock.NewMock()

	torrent, cleanup := storage.TorrentFixture(core.SizedBlobFixture(1, 1).MetaInfo)
	defer cleanup()

	d := testDispatcher(config, clk, torrent)

	p1, err := d.addPeer(core.PeerIDFixture(), storage.BitSetFixture(true), newMockMessages())
	require.NoError(err)

	d.maybeRequestMorePieces(p1)
	require.Equal(map[int]int{0: 1}, numRequestsPerPiece(p1.messages))

	p2, err := d.addPeer(core.PeerIDFixture(), storage.BitSetFixture(true), newMockMessages())
	require.NoError(err)

	// Should send duplicate request for piece 0 since we're in endgame.
	d.maybeRequestMorePieces(p2)
	require.Equal(map[int]int{0: 1}, numRequestsPerPiece(p2.messages))
}

func TestDispatcherHandlePiecePayloadAnnouncesPiece(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(2, 1)

	torrent, cleanup := storage.TorrentFixture(blob.MetaInfo)
	defer cleanup()

	d := testDispatcher(Config{}, clock.NewMock(), torrent)

	p1, err := d.addPeer(core.PeerIDFixture(), storage.BitSetFixture(false, false), newMockMessages())
	require.NoError(err)

	p2, err := d.addPeer(core.PeerIDFixture(), storage.BitSetFixture(false, false), newMockMessages())
	require.NoError(err)

	msg := conn.NewPiecePayloadMessage(0, storage.NewPieceReaderBuffer(blob.Content[0:1]))

	require.NoError(d.dispatch(p1, msg))

	// Should not announce to the peer who sent the payload.
	require.Empty(announcedPieces(p1.messages))

	// Should announce to other peers.
	require.Equal([]int{0}, announcedPieces(p2.messages))
}

func TestDispatcherHandlePiecePayloadSendsCompleteMessage(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(1, 1)

	torrent, cleanup := storage.TorrentFixture(blob.MetaInfo)
	defer cleanup()

	d := testDispatcher(Config{}, clock.NewMock(), torrent)

	p1, err := d.addPeer(core.PeerIDFixture(), storage.BitSetFixture(false), newMockMessages())
	require.NoError(err)

	p2, err := d.addPeer(core.PeerIDFixture(), storage.BitSetFixture(false), newMockMessages())
	require.NoError(err)

	msg := conn.NewPiecePayloadMessage(0, storage.NewPieceReaderBuffer(blob.Content[0:1]))

	require.NoError(d.dispatch(p1, msg))

	require.True(hasComplete(p1.messages))
	require.True(hasComplete(p2.messages))
}

func TestDispatcherClosesCompletedPeersWhenComplete(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(1, 1)

	torrent, cleanup := storage.TorrentFixture(blob.MetaInfo)
	defer cleanup()

	d := testDispatcher(Config{}, clock.NewMock(), torrent)

	completedPeer, err := d.addPeer(core.PeerIDFixture(), storage.BitSetFixture(true), newMockMessages())
	require.NoError(err)

	incompletePeer, err := d.addPeer(
		core.PeerIDFixture(), storage.BitSetFixture(false), newMockMessages())
	require.NoError(err)

	msg := conn.NewPiecePayloadMessage(0, storage.NewPieceReaderBuffer(blob.Content[0:1]))

	// Completed peers are closed when the dispatcher completes.
	require.NoError(d.dispatch(completedPeer, msg))
	require.True(closed(completedPeer.messages))
	require.False(closed(incompletePeer.messages))

	// Peers which send complete messages are closed if the dispatcher is complete.
	require.NoError(d.dispatch(incompletePeer, conn.NewCompleteMessage()))
	require.True(closed(incompletePeer.messages))
}

func TestDispatcherHandleCompleteRequestsPieces(t *testing.T) {
	require := require.New(t)

	blob := core.SizedBlobFixture(1, 1)

	torrent, cleanup := storage.TorrentFixture(blob.MetaInfo)
	defer cleanup()

	d := testDispatcher(Config{}, clock.NewMock(), torrent)

	p, err := d.addPeer(core.PeerIDFixture(), storage.BitSetFixture(false), newMockMessages())
	require.NoError(err)

	require.Empty(numRequestsPerPiece(p.messages))

	require.NoError(d.dispatch(p, conn.NewCompleteMessage()))

	require.Equal(map[int]int{0: 1}, numRequestsPerPiece(p.messages))
	require.False(closed(p.messages))
}
