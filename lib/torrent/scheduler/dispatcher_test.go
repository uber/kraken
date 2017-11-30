package scheduler

import (
	"errors"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/.gen/go/p2p"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/memsize"
	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

type mockMessages struct {
	sent     []*message
	receiver chan *message
	closed   bool
}

func newMockMessages() *mockMessages {
	return &mockMessages{receiver: make(chan *message)}
}

func (m *mockMessages) Send(msg *message) error {
	if m.closed {
		return errors.New("messages closed")
	}
	m.sent = append(m.sent, msg)
	return nil
}

func (m *mockMessages) Receiver() <-chan *message { return m.receiver }

func (m *mockMessages) Close() {
	if m.closed {
		return
	}
	close(m.receiver)
	m.closed = true
}

func (m *mockMessages) numRequestsPerPiece() map[int]int {
	requests := make(map[int]int)
	for _, msg := range m.sent {
		if msg.Message.Type == p2p.Message_PIECE_REQUEST {
			requests[int(msg.Message.PieceRequest.Index)]++
		}
	}
	return requests
}

func TestDispatcherDoesNotSendDuplicateInitialPieceRequests(t *testing.T) {
	require := require.New(t)

	config := dispatcherConfigFixture()
	clk := clock.NewMock()
	f := dispatcherFactoryFixture(config, clk)

	torrent, cleanup := storage.TorrentFixture(100, 1)
	defer cleanup()

	d := f.init(torrent)

	var mu sync.Mutex
	numRequestsPerPiece := make(map[int]int)

	// Add a bunch of peers concurrently which are saturated with pieces d needs.
	// We should send exactly one piece request per piece.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var bitfield storage.Bitfield
			for i := 0; i < torrent.NumPieces(); i++ {
				bitfield = append(bitfield, true)
			}
			messages := newMockMessages()
			p, err := d.addPeer(torlib.PeerIDFixture(), bitfield, messages)
			require.NoError(err)
			d.sendInitialPieceRequests(p)
			for i, n := range messages.numRequestsPerPiece() {
				mu.Lock()
				numRequestsPerPiece[i] += n
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	for i := 0; i < 100; i++ {
		require.Equal(1, numRequestsPerPiece[i], "piece: %d", i)
	}
}

func TestDispatcherReservePieceRequest(t *testing.T) {
	require := require.New(t)

	config := dispatcherConfigFixture()
	clk := clock.NewMock()
	f := dispatcherFactoryFixture(config, clk)

	torrent, cleanup := storage.TorrentFixture(100, 1)
	defer cleanup()

	d := f.init(torrent)

	peerID := torlib.PeerIDFixture()

	require.True(d.reservePieceRequest(peerID, 0))

	// Further reservations fail.
	require.False(d.reservePieceRequest(peerID, 0))
	require.False(d.reservePieceRequest(torlib.PeerIDFixture(), 0))

	clk.Add(d.pieceRequestTimeout + 1)

	require.True(d.reservePieceRequest(peerID, 0))
}

func TestDispatcherResendExpiredPieceRequests(t *testing.T) {
	require := require.New(t)

	config := dispatcherConfigFixture()
	clk := clock.NewMock()
	f := dispatcherFactoryFixture(config, clk)

	torrent, cleanup := storage.TorrentFixture(2, 1)
	defer cleanup()

	d := f.init(torrent)

	// p1 has both pieces and sends requests for both.
	p1Messages := newMockMessages()
	p1, err := d.addPeer(
		torlib.PeerIDFixture(), []bool{true, true}, p1Messages)
	require.NoError(err)
	d.sendInitialPieceRequests(p1)
	require.Equal(map[int]int{
		0: 1,
		1: 1,
	}, p1Messages.numRequestsPerPiece())

	// p2 has piece 0 and sends no piece requests.
	p2Messages := newMockMessages()
	p2, err := d.addPeer(
		torlib.PeerIDFixture(), []bool{true, false}, p2Messages)
	require.NoError(err)
	d.sendInitialPieceRequests(p2)
	require.Equal(map[int]int{}, p2Messages.numRequestsPerPiece())

	// p3 has piece 1 and sends no piece requests.
	p3Messages := newMockMessages()
	p3, err := d.addPeer(
		torlib.PeerIDFixture(), []bool{false, true}, p3Messages)
	require.NoError(err)
	d.sendInitialPieceRequests(p3)
	require.Equal(map[int]int{}, p3Messages.numRequestsPerPiece())

	clk.Add(d.pieceRequestTimeout + 1)

	d.resendExpiredPieceRequests()

	// p1 was not sent any new piece requests.
	require.Equal(map[int]int{
		0: 1,
		1: 1,
	}, p1Messages.numRequestsPerPiece())

	// p2 was sent a piece request for piece 0.
	require.Equal(map[int]int{
		0: 1,
	}, p2Messages.numRequestsPerPiece())

	// p3 was sent a piece request for piece 1.
	require.Equal(map[int]int{
		1: 1,
	}, p3Messages.numRequestsPerPiece())
}

func TestDispatcherSendErrorsClearPendingPieceRequest(t *testing.T) {
	require := require.New(t)

	config := dispatcherConfigFixture()
	clk := clock.NewMock()
	f := dispatcherFactoryFixture(config, clk)

	torrent, cleanup := storage.TorrentFixture(1, 1)
	defer cleanup()

	d := f.init(torrent)

	p1Messages := newMockMessages()
	p1, err := d.addPeer(torlib.PeerIDFixture(), []bool{true}, p1Messages)
	require.NoError(err)

	p1Messages.Close()

	// Send should fail since p1 messages are closed.
	d.sendInitialPieceRequests(p1)

	require.Equal(map[int]int{}, p1Messages.numRequestsPerPiece())

	p2Messages := newMockMessages()
	p2, err := d.addPeer(torlib.PeerIDFixture(), []bool{true}, p2Messages)
	require.NoError(err)

	// Send should succeed since pending requests were cleared.
	d.sendInitialPieceRequests(p2)

	require.Equal(map[int]int{
		0: 1,
	}, p2Messages.numRequestsPerPiece())
}

func TestCalcPieceRequestTimeout(t *testing.T) {
	config := dispatcherConfigFixture()
	config.PieceRequestMinTimeout = 5 * time.Second
	config.PieceRequestTimeoutPerMb = 2 * time.Second

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
			f := dispatcherFactoryFixture(config, clock.New())
			timeout := f.calcPieceRequestTimeout(int64(test.maxPieceLength))
			require.Equal(t, test.expected, timeout)
		})
	}
}
