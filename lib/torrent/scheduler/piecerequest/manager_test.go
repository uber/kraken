package piecerequest

import (
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/torlib"
)

func TestManagerReserveExpiredRequest(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	timeout := 5 * time.Second

	m := NewManager(clk, timeout)

	peerID := torlib.PeerIDFixture()

	require.True(m.Reserve(peerID, 0))

	// Further reservations fail.
	require.False(m.Reserve(peerID, 0))
	require.False(m.Reserve(torlib.PeerIDFixture(), 0))

	clk.Add(timeout + 1)

	require.True(m.Reserve(peerID, 0))
}

func TestManagerReserveUnsentRequest(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second)

	peerID := torlib.PeerIDFixture()

	require.True(m.Reserve(peerID, 0))

	// Further reservations fail.
	require.False(m.Reserve(peerID, 0))
	require.False(m.Reserve(torlib.PeerIDFixture(), 0))

	m.MarkUnsent(peerID, 0)

	require.True(m.Reserve(peerID, 0))
}

func TestManagerReserveInvalidRequest(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second)

	peerID := torlib.PeerIDFixture()

	require.True(m.Reserve(peerID, 0))

	// Further reservations fail.
	require.False(m.Reserve(peerID, 0))
	require.False(m.Reserve(torlib.PeerIDFixture(), 0))

	m.MarkInvalid(peerID, 0)

	require.True(m.Reserve(peerID, 0))
}

func TestManagerGetFailedRequests(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	timeout := 5 * time.Second

	m := NewManager(clk, timeout)

	p0 := torlib.PeerIDFixture()
	p1 := torlib.PeerIDFixture()
	p2 := torlib.PeerIDFixture()

	for i, p := range []torlib.PeerID{p0, p1, p2} {
		require.True(m.Reserve(p, i))
	}

	m.MarkUnsent(p0, 0)
	m.MarkInvalid(p1, 1)
	clk.Add(timeout + 1) // Expires p2's request.

	p3 := torlib.PeerIDFixture()
	require.True(m.Reserve(p3, 3))

	failed := m.GetFailedRequests()

	require.Len(failed, 3)
	require.Contains(failed, Request{Piece: 0, PeerID: p0, Status: StatusUnsent})
	require.Contains(failed, Request{Piece: 1, PeerID: p1, Status: StatusInvalid})
	require.Contains(failed, Request{Piece: 2, PeerID: p2, Status: StatusExpired})
}

func TestManagerClear(t *testing.T) {
	require := require.New(t)

	m := NewManager(clock.NewMock(), 5*time.Second)

	require.True(m.Reserve(torlib.PeerIDFixture(), 0))

	require.Len(m.requests, 1)

	m.Clear(0)

	require.Len(m.requests, 0)
}
