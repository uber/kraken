// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package piecerequest

import (
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/bitsetutil"
	"github.com/uber/kraken/utils/syncutil"
)

func newManager(
	clk clock.Clock,
	timeout time.Duration,
	policy string,
	pipelineLimit int) *Manager {

	m, err := NewManager(clk, timeout, policy, pipelineLimit)
	if err != nil {
		panic(err)
	}
	return m
}

func countsFromInts(priorities ...int) syncutil.Counters {
	c := syncutil.NewCounters(len(priorities))
	for i, p := range priorities {
		c.Set(i, p)
	}

	return c
}

func TestManagerPipelineLimit(t *testing.T) {
	require := require.New(t)

	m := newManager(clock.NewMock(), 5*time.Second, DefaultPolicy, 3)

	peerID := core.PeerIDFixture()

	pieces, err := m.ReservePieces(peerID, bitsetutil.FromBools(true, true, true, true),
		countsFromInts(0, 0, 0, 0), false)
	require.NoError(err)
	require.Len(pieces, 3)

	require.Len(m.PendingPieces(peerID), 3)
}

func TestManagerReserveExpiredRequest(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	timeout := 5 * time.Second

	m := newManager(clk, timeout, DefaultPolicy, 1)

	peerID := core.PeerIDFixture()

	pieces, err := m.ReservePieces(peerID, bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Equal([]int{0}, pieces)

	// Further reservations fail.
	pieces, err = m.ReservePieces(peerID, bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Empty(pieces)

	pieces, err = m.ReservePieces(core.PeerIDFixture(), bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Empty(pieces)

	clk.Add(timeout + 1)

	pieces, err = m.ReservePieces(peerID, bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Equal([]int{0}, pieces)
}

func TestManagerReserveUnsentRequest(t *testing.T) {
	require := require.New(t)

	m := newManager(clock.NewMock(), 5*time.Second, DefaultPolicy, 1)

	peerID := core.PeerIDFixture()

	pieces, err := m.ReservePieces(peerID, bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Equal([]int{0}, pieces)

	// Further reservations fail.
	pieces, err = m.ReservePieces(peerID, bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Empty(pieces)

	pieces, err = m.ReservePieces(core.PeerIDFixture(), bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Empty(pieces)

	m.MarkUnsent(peerID, 0)

	pieces, err = m.ReservePieces(peerID, bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Equal([]int{0}, pieces)
}

func TestManagerReserveInvalidRequest(t *testing.T) {
	require := require.New(t)

	m := newManager(clock.NewMock(), 5*time.Second, DefaultPolicy, 1)

	peerID := core.PeerIDFixture()

	pieces, err := m.ReservePieces(peerID, bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Equal([]int{0}, pieces)

	// Further reservations fail.
	pieces, err = m.ReservePieces(peerID, bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Empty(pieces)

	pieces, err = m.ReservePieces(core.PeerIDFixture(), bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Empty(pieces)

	m.MarkInvalid(peerID, 0)

	pieces, err = m.ReservePieces(peerID, bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Equal([]int{0}, pieces)
}

func TestManagerGetFailedRequests(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	timeout := 5 * time.Second

	m := newManager(clk, timeout, RarestFirstPolicy, 1)

	p0 := core.PeerIDFixture()
	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()

	pieces, err := m.ReservePieces(p0, bitsetutil.FromBools(true, true, true),
		countsFromInts(0, 1, 2), false)
	require.NoError(err)
	require.Equal([]int{0}, pieces)

	pieces, err = m.ReservePieces(p1, bitsetutil.FromBools(false, true, false),
		countsFromInts(0, 1, 2), false)
	require.NoError(err)
	require.Equal([]int{1}, pieces)

	pieces, err = m.ReservePieces(p2, bitsetutil.FromBools(false, false, true),
		countsFromInts(0, 1, 2), false)
	require.NoError(err)
	require.Equal([]int{2}, pieces)

	m.MarkUnsent(p0, 0)
	m.MarkInvalid(p1, 1)
	clk.Add(timeout + 1) // Expires p2's request.

	p3 := core.PeerIDFixture()
	pieces, err = m.ReservePieces(p3, bitsetutil.FromBools(false, false, false, true),
		countsFromInts(0, 0, 0, 0), false)
	require.NoError(err)
	require.Equal([]int{3}, pieces)

	failed := m.GetFailedRequests()

	require.Len(failed, 3)
	require.Contains(failed, Request{Piece: 0, PeerID: p0, Status: StatusUnsent})
	require.Contains(failed, Request{Piece: 1, PeerID: p1, Status: StatusInvalid})
	require.Contains(failed, Request{Piece: 2, PeerID: p2, Status: StatusExpired})
}

func TestManagerClear(t *testing.T) {
	require := require.New(t)

	m := newManager(clock.NewMock(), 5*time.Second, DefaultPolicy, 1)

	peerID := core.PeerIDFixture()

	pieces, err := m.ReservePieces(peerID, bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Equal([]int{0}, pieces)

	require.Len(m.PendingPieces(peerID), 1)

	m.Clear(0)

	require.Empty(m.PendingPieces(peerID))
}

func TestManagerClearPeer(t *testing.T) {
	require := require.New(t)

	m := newManager(clock.NewMock(), 5*time.Second, DefaultPolicy, 1)

	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()

	pieces, err := m.ReservePieces(p1, bitsetutil.FromBools(true),
		countsFromInts(0), false)
	require.NoError(err)
	require.Equal([]int{0}, pieces)

	pieces, err = m.ReservePieces(p1, bitsetutil.FromBools(true, true),
		countsFromInts(0, 1), false)
	require.NoError(err)
	require.Empty(pieces)

	pieces, err = m.ReservePieces(p2, bitsetutil.FromBools(true, true),
		countsFromInts(0, 1), false)
	require.NoError(err)
	require.Equal([]int{1}, pieces)

	m.ClearPeer(p1)

	require.Empty(m.PendingPieces(p1))
	require.Equal([]int{1}, m.PendingPieces(p2))
}

func TestManagerReservePiecesAllowDuplicate(t *testing.T) {
	require := require.New(t)

	m := newManager(clock.NewMock(), 5*time.Second, DefaultPolicy, 2)

	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()

	pieces, err := m.ReservePieces(p1, bitsetutil.FromBools(true),
		countsFromInts(0), true)
	require.NoError(err)
	require.Equal([]int{0}, pieces)

	// Shouldn't allow duplicates on the same peer.
	pieces, err = m.ReservePieces(p1, bitsetutil.FromBools(true),
		countsFromInts(0), true)
	require.NoError(err)
	require.Empty(pieces)

	// Should allow duplicates for different peers.
	pieces, err = m.ReservePieces(p2, bitsetutil.FromBools(true),
		countsFromInts(0), true)
	require.NoError(err)
	require.Equal([]int{0}, pieces)
}

func TestManagerClearWhenAllowedDuplicates(t *testing.T) {
	require := require.New(t)

	m := newManager(clock.NewMock(), 5*time.Second, DefaultPolicy, 2)

	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()

	pieces, err := m.ReservePieces(p1, bitsetutil.FromBools(true, true),
		countsFromInts(0, 0), true)
	require.NoError(err)
	require.Equal([]int{0, 1}, pieces)

	pieces, err = m.ReservePieces(p2, bitsetutil.FromBools(true, true),
		countsFromInts(0, 0), true)
	require.NoError(err)
	require.Equal([]int{0, 1}, pieces)

	m.Clear(0)

	require.Equal([]int{1}, m.PendingPieces(p1))
	require.Equal([]int{1}, m.PendingPieces(p2))
}

func TestManagerClearPeerWhenAllowedDuplicates(t *testing.T) {
	require := require.New(t)

	m := newManager(clock.NewMock(), 5*time.Second, DefaultPolicy, 2)

	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()

	pieces, err := m.ReservePieces(p1, bitsetutil.FromBools(true, true),
		countsFromInts(0, 0), true)
	require.NoError(err)
	require.Equal([]int{0, 1}, pieces)

	pieces, err = m.ReservePieces(p2, bitsetutil.FromBools(true, true),
		countsFromInts(0, 0), true)
	require.NoError(err)
	require.Equal([]int{0, 1}, pieces)

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

			m := newManager(clock.NewMock(), 5*time.Second, DefaultPolicy, 2)

			p1 := core.PeerIDFixture()
			p2 := core.PeerIDFixture()

			pieces, err := m.ReservePieces(p1, bitsetutil.FromBools(true, true),
				countsFromInts(0, 0), true)
			require.NoError(err)
			require.Equal([]int{0, 1}, pieces)

			pieces, err = m.ReservePieces(p2, bitsetutil.FromBools(true, true),
				countsFromInts(0, 0), true)
			require.NoError(err)
			require.Equal([]int{0, 1}, pieces)

			test.mark(m, p1, 0)

			require.Equal([]int{1}, m.PendingPieces(p1))
			require.Equal([]int{0, 1}, m.PendingPieces(p2))
		})
	}
}

func TestRarestFirstPolicy(t *testing.T) {
	require := require.New(t)

	m := newManager(clock.NewMock(), 5*time.Second, RarestFirstPolicy, 2)

	p1 := core.PeerIDFixture()
	p2 := core.PeerIDFixture()
	p3 := core.PeerIDFixture()

	pieces, err := m.ReservePieces(p1, bitsetutil.FromBools(true, true, false, true),
		countsFromInts(2, 3, 1, 0), false)
	require.NoError(err)
	require.Equal([]int{3, 0}, pieces)

	pieces, err = m.ReservePieces(p2, bitsetutil.FromBools(true, true, false, true),
		countsFromInts(2, 3, 1, 0), false)
	require.NoError(err)
	require.Equal([]int{1}, pieces)

	pieces, err = m.ReservePieces(p3, bitsetutil.FromBools(true, true, false, true),
		countsFromInts(2, 3, 1, 0), false)
	require.NoError(err)
	require.Empty(pieces)

	pieces, err = m.ReservePieces(p1, bitsetutil.FromBools(true, true, false, true),
		countsFromInts(2, 3, 1, 0), false)
	require.NoError(err)
	require.Empty(pieces)

	m.MarkUnsent(p1, 3)
	pieces, err = m.ReservePieces(p2, bitsetutil.FromBools(true, true, false, true),
		countsFromInts(2, 3, 1, 0), false)
	require.NoError(err)
	require.Equal([]int{3}, pieces)

	m.MarkUnsent(p1, 0)
	pieces, err = m.ReservePieces(p2, bitsetutil.FromBools(true, true, false, true),
		countsFromInts(2, 3, 1, 0), false)
	require.NoError(err)
	require.Empty(pieces)
}
