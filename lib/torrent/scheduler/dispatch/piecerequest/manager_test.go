package piecerequest

import (
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/bitsetutil"
)

func TestManagerPipelineLimit(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 3)

	peerID := core.PeerIDFixture()

	require.Len(
		m.ReservePieces(peerID, bitsetutil.FromBools(true, true, true, true), false),
		3)

	require.Len(m.PendingPieces(peerID), 3)
}

func TestManagerReserveExpiredRequest(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	timeout := 5 * time.Second

	m := NewManager(clk, timeout, 1)

	peerID := core.PeerIDFixture()

	require.Equal(
		[]int{0}, m.ReservePieces(peerID, bitsetutil.FromBools(true), false))

	// Further reservations fail.
	require.Empty(m.ReservePieces(peerID, bitsetutil.FromBools(true), false))
	require.Empty(m.ReservePieces(core.PeerIDFixture(), bitsetutil.FromBools(true), false))

	clk.Add(timeout + 1)

	require.Equal(
		[]int{0}, m.ReservePieces(peerID, bitsetutil.FromBools(true), false))
}

func TestManagerReserveUnsentRequest(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 1)

	peerID := core.PeerIDFixture()

	require.Equal(
		[]int{0}, m.ReservePieces(peerID, bitsetutil.FromBools(true), false))

	// Further reservations fail.
	require.Empty(m.ReservePieces(peerID, bitsetutil.FromBools(true), false))
	require.Empty(m.ReservePieces(core.PeerIDFixture(), bitsetutil.FromBools(true), false))

	m.MarkUnsent(peerID, 0)

	require.Equal(
		[]int{0}, m.ReservePieces(peerID, bitsetutil.FromBools(true), false))
}

func TestManagerReserveInvalidRequest(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 1)

	peerID := core.PeerIDFixture()

	require.Equal(
		[]int{0}, m.ReservePieces(peerID, bitsetutil.FromBools(true), false))

	// Further reservations fail.
	require.Empty(m.ReservePieces(peerID, bitsetutil.FromBools(true), false))
	require.Empty(m.ReservePieces(core.PeerIDFixture(), bitsetutil.FromBools(true), false))

	m.MarkInvalid(peerID, 0)

	require.Equal(
		[]int{0}, m.ReservePieces(peerID, bitsetutil.FromBools(true), false))
}

func TestManagerGetFailedRequests(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	timeout := 5 * time.Second

	m := NewManager(clk, timeout, 1)

	p0 := core.PeerIDFixture()
	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()

	require.Equal(
		[]int{0}, m.ReservePieces(p0, bitsetutil.FromBools(true, false, false), false))
	require.Equal(
		[]int{1}, m.ReservePieces(p1, bitsetutil.FromBools(false, true, false), false))
	require.Equal(
		[]int{2}, m.ReservePieces(p2, bitsetutil.FromBools(false, false, true), false))

	m.MarkUnsent(p0, 0)
	m.MarkInvalid(p1, 1)
	clk.Add(timeout + 1) // Expires p2's request.

	p3 := core.PeerIDFixture()
	require.Equal(
		[]int{3}, m.ReservePieces(p3, bitsetutil.FromBools(false, false, false, true), false))

	failed := m.GetFailedRequests()

	require.Len(failed, 3)
	require.Contains(failed, Request{Piece: 0, PeerID: p0, Status: StatusUnsent})
	require.Contains(failed, Request{Piece: 1, PeerID: p1, Status: StatusInvalid})
	require.Contains(failed, Request{Piece: 2, PeerID: p2, Status: StatusExpired})
}

func TestManagerClear(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 1)

	peerID := core.PeerIDFixture()

	require.Equal(
		[]int{0}, m.ReservePieces(peerID, bitsetutil.FromBools(true), false))

	require.Len(m.PendingPieces(peerID), 1)

	m.Clear(0)

	require.Empty(m.PendingPieces(peerID))
}

func TestManagerClearPeer(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 1)

	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()

	require.Equal(
		[]int{0}, m.ReservePieces(p1, bitsetutil.FromBools(true), false))
	require.Equal(
		[]int{1}, m.ReservePieces(p1, bitsetutil.FromBools(false, true), false))
	require.Equal(
		[]int{2}, m.ReservePieces(p2, bitsetutil.FromBools(false, false, true), false))

	m.ClearPeer(p1)

	require.Empty(m.PendingPieces(p1))
	require.Equal([]int{2}, m.PendingPieces(p2))
}

func TestManagerReservePiecesAllowDuplicate(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 2)

	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()

	require.Equal(
		[]int{0}, m.ReservePieces(p1, bitsetutil.FromBools(true), true))

	// Shouldn't allow duplicates on the same peer.
	require.Empty(
		m.ReservePieces(p1, bitsetutil.FromBools(true), true))

	// Should allow duplicates for different peers.
	require.Equal(
		[]int{0}, m.ReservePieces(p2, bitsetutil.FromBools(true), true))
}

func TestManagerClearWhenAllowedDuplicates(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 2)

	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()

	require.Equal(
		[]int{0, 1}, m.ReservePieces(p1, bitsetutil.FromBools(true, true), true))
	require.Equal(
		[]int{0, 1}, m.ReservePieces(p2, bitsetutil.FromBools(true, true), true))

	m.Clear(0)

	require.Equal([]int{1}, m.PendingPieces(p1))
	require.Equal([]int{1}, m.PendingPieces(p2))
}

func TestManagerClearPeerWhenAllowedDuplicates(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second, 2)

	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()

	require.Equal(
		[]int{0, 1}, m.ReservePieces(p1, bitsetutil.FromBools(true, true), true))
	require.Equal(
		[]int{0, 1}, m.ReservePieces(p2, bitsetutil.FromBools(true, true), true))

	m.ClearPeer(p1)

	require.Empty(m.PendingPieces(p1))
	require.Equal([]int{0, 1}, m.PendingPieces(p2))
}

func TestManagerMarkStatusWhenAllowedDuplicates(t *testing.T) {
	tests := []struct {
		desc string
		mark func(*Manager, core.PeerID, int)
	}{
		{
			"mark unsent",
			func(m *Manager, p core.PeerID, i int) { m.MarkUnsent(p, i) },
		}, {
			"mark invalid",
			func(m *Manager, p core.PeerID, i int) { m.MarkInvalid(p, i) },
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			m := NewManager(clock.NewMock(), 5*time.Second, 2)

			p1 := core.PeerIDFixture()
			p2 := core.PeerIDFixture()

			require.Equal(
				[]int{0, 1}, m.ReservePieces(p1, bitsetutil.FromBools(true, true), true))
			require.Equal(
				[]int{0, 1}, m.ReservePieces(p2, bitsetutil.FromBools(true, true), true))

			test.mark(m, p1, 0)

			require.Equal([]int{1}, m.PendingPieces(p1))
			require.Equal([]int{0, 1}, m.PendingPieces(p2))
		})
	}
}
