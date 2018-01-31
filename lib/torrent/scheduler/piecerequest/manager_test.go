package piecerequest

import (
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
)

func TestManagerPipelineLimit(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 3)

	pieces1 := m.ReservePieces(
		torlib.PeerIDFixture(), storage.BitSetFixture(true, true, true, true))
	require.Len(pieces1, 3)

	require.Len(m.requests, 3)
}

func TestManagerReserveExpiredRequest(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	timeout := 5 * time.Second

	m := NewManager(clk, timeout, 1)

	peerID := torlib.PeerIDFixture()

	pieces1 := m.ReservePieces(peerID, storage.BitSetFixture(true))
	require.Equal([]int{0}, pieces1)

	// Further reservations fail.
	require.Empty(m.ReservePieces(peerID, storage.BitSetFixture(true)))
	require.Empty(m.ReservePieces(torlib.PeerIDFixture(), storage.BitSetFixture(true)))

	clk.Add(timeout + 1)

	pieces2 := m.ReservePieces(peerID, storage.BitSetFixture(true))
	require.Equal([]int{0}, pieces2)
}

func TestManagerReserveUnsentRequest(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 1)

	peerID := torlib.PeerIDFixture()

	pieces1 := m.ReservePieces(peerID, storage.BitSetFixture(true))
	require.Equal([]int{0}, pieces1)

	// Further reservations fail.
	require.Empty(m.ReservePieces(peerID, storage.BitSetFixture(true)))
	require.Empty(m.ReservePieces(torlib.PeerIDFixture(), storage.BitSetFixture(true)))

	m.MarkUnsent(peerID, 0)

	pieces2 := m.ReservePieces(peerID, storage.BitSetFixture(true))
	require.Equal([]int{0}, pieces2)
}

func TestManagerReserveInvalidRequest(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 1)

	peerID := torlib.PeerIDFixture()

	pieces1 := m.ReservePieces(peerID, storage.BitSetFixture(true))
	require.Equal([]int{0}, pieces1)

	// Further reservations fail.
	require.Empty(m.ReservePieces(peerID, storage.BitSetFixture(true)))
	require.Empty(m.ReservePieces(torlib.PeerIDFixture(), storage.BitSetFixture(true)))

	m.MarkInvalid(peerID, 0)

	pieces2 := m.ReservePieces(peerID, storage.BitSetFixture(true))
	require.Equal([]int{0}, pieces2)
}

func TestManagerGetFailedRequests(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	timeout := 5 * time.Second

	m := NewManager(clk, timeout, 1)

	p0 := torlib.PeerIDFixture()
	p1 := torlib.PeerIDFixture()
	p2 := torlib.PeerIDFixture()

	pieces0 := m.ReservePieces(p0, storage.BitSetFixture(true, false, false))
	require.Equal([]int{0}, pieces0)
	pieces1 := m.ReservePieces(p1, storage.BitSetFixture(false, true, false))
	require.Equal([]int{1}, pieces1)
	pieces2 := m.ReservePieces(p2, storage.BitSetFixture(false, false, true))
	require.Equal([]int{2}, pieces2)

	m.MarkUnsent(p0, 0)
	m.MarkInvalid(p1, 1)
	clk.Add(timeout + 1) // Expires p2's request.

	p3 := torlib.PeerIDFixture()
	pieces3 := m.ReservePieces(p3, storage.BitSetFixture(false, false, false, true))
	require.Equal([]int{3}, pieces3)

	failed := m.GetFailedRequests()

	require.Len(failed, 3)
	require.Contains(failed, Request{Piece: 0, PeerID: p0, Status: StatusUnsent})
	require.Contains(failed, Request{Piece: 1, PeerID: p1, Status: StatusInvalid})
	require.Contains(failed, Request{Piece: 2, PeerID: p2, Status: StatusExpired})
}

func TestManagerClear(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 1)

	pieces1 := m.ReservePieces(torlib.PeerIDFixture(), storage.BitSetFixture(true))
	require.Equal([]int{0}, pieces1)

	require.Len(m.requests, 1)

	m.Clear(0)

	require.Len(m.requests, 0)
}

func TestManagerClearPeer(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 1)

	p1 := torlib.PeerIDFixture()
	p2 := torlib.PeerIDFixture()

	pieces0 := m.ReservePieces(p1, storage.BitSetFixture(true))
	require.Equal([]int{0}, pieces0)
	pieces1 := m.ReservePieces(p1, storage.BitSetFixture(false, true))
	require.Equal([]int{1}, pieces1)
	pieces2 := m.ReservePieces(p2, storage.BitSetFixture(false, false, true))
	require.Equal([]int{2}, pieces2)

	m.ClearPeer(p1)

	require.Nil(m.requests[0])
	require.Nil(m.requests[1])
	require.NotNil(m.requests[2])
}
