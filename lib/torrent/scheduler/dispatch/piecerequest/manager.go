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
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/syncutil"

	"github.com/andres-erbsen/clock"
	"github.com/willf/bitset"
)

// Status enumerates possible statuses of a Request.
type Status int

const (
	// StatusPending denotes a valid request which is still in-flight.
	StatusPending Status = iota

	// StatusExpired denotes an in-flight request which has timed out on our end.
	StatusExpired

	// StatusUnsent denotes an unsent request that is safe to retry to the same peer.
	StatusUnsent

	// StatusInvalid denotes a completed request that resulted in an invalid payload.
	StatusInvalid
)

// Request represents a piece request to peer.
type Request struct {
	Piece  int
	PeerID core.PeerID
	Status Status

	sentAt time.Time
}

// Manager encapsulates thread-safe piece request bookkeeping. It is not responsible
// for sending nor receiving pieces in any way.
type Manager struct {
	sync.RWMutex

	// requests and requestsByPeer holds the same data, just indexed differently.
	requests       map[int][]*Request
	requestsByPeer map[core.PeerID]map[int]*Request

	clock   clock.Clock
	timeout time.Duration

	policy        pieceSelectionPolicy
	pipelineLimit int
}

// NewManager creates a new Manager.
func NewManager(
	clk clock.Clock,
	timeout time.Duration,
	policy string,
	pipelineLimit int) (*Manager, error) {

	m := &Manager{
		requests:       make(map[int][]*Request),
		requestsByPeer: make(map[core.PeerID]map[int]*Request),
		clock:          clk,
		timeout:        timeout,
		pipelineLimit:  pipelineLimit,
	}

	switch policy {
	case DefaultPolicy:
		m.policy = newDefaultPolicy()
	case RarestFirstPolicy:
		m.policy = newRarestFirstPolicy()
	default:
		return nil, fmt.Errorf("invalid piece selection policy: %s", policy)
	}
	return m, nil
}

// ReservePieces selects the next piece(s) to be requested from given peer.
// It selects peers on a rarity-first basis using numPeersByPiece.
// If allowDuplicates is set, may return pieces which have already been
// reserved under other peers.
func (m *Manager) ReservePieces(
	peerID core.PeerID,
	candidates *bitset.BitSet,
	numPeersByPiece syncutil.Counters,
	allowDuplicates bool) ([]int, error) {

	m.Lock()
	defer m.Unlock()

	quota := m.requestQuota(peerID)
	if quota <= 0 {
		return nil, nil
	}

	valid := func(i int) bool { return m.validRequest(peerID, i, allowDuplicates) }
	pieces, err := m.policy.selectPieces(quota, valid, candidates, numPeersByPiece)
	if err != nil {
		return nil, err
	}

	// Set as pending in requests map.
	for _, i := range pieces {
		r := &Request{
			Piece:  i,
			PeerID: peerID,
			Status: StatusPending,
			sentAt: m.clock.Now(),
		}
		m.requests[i] = append(m.requests[i], r)
		if _, ok := m.requestsByPeer[peerID]; !ok {
			m.requestsByPeer[peerID] = make(map[int]*Request)
		}
		m.requestsByPeer[peerID][i] = r
	}

	return pieces, nil
}

// MarkUnsent marks the piece request for piece i as unsent.
func (m *Manager) MarkUnsent(peerID core.PeerID, i int) {
	m.markStatus(peerID, i, StatusUnsent)
}

// MarkInvalid marks the piece request for piece i as invalid.
func (m *Manager) MarkInvalid(peerID core.PeerID, i int) {
	m.markStatus(peerID, i, StatusInvalid)
}

// Clear deletes the piece request for piece i. Should be used for freeing up
// unneeded request bookkeeping.
func (m *Manager) Clear(i int) {
	m.Lock()
	defer m.Unlock()

	delete(m.requests, i)

	for peerID, pm := range m.requestsByPeer {
		delete(pm, i)
		if len(pm) == 0 {
			delete(m.requestsByPeer, peerID)
		}
	}
}

// PendingPieces returns the pieces for all pending requests to peerID in sorted
// order. Intended primarily for testing purposes.
func (m *Manager) PendingPieces(peerID core.PeerID) []int {
	m.RLock()
	defer m.RUnlock()

	var pieces []int
	for i, r := range m.requestsByPeer[peerID] {
		if r.Status == StatusPending {
			pieces = append(pieces, i)
		}
	}
	sort.Ints(pieces)
	return pieces
}

// ClearPeer deletes all piece requests for peerID.
func (m *Manager) ClearPeer(peerID core.PeerID) {
	m.Lock()
	defer m.Unlock()

	delete(m.requestsByPeer, peerID)

	for i, rs := range m.requests {
		for j, r := range rs {
			if r.PeerID == peerID {
				// Eject request.
				rs[j] = rs[len(rs)-1]
				m.requests[i] = rs[:len(rs)-1]
				break
			}
		}
	}
}

// GetFailedRequests returns a copy of all failed piece requests.
func (m *Manager) GetFailedRequests() []Request {
	m.RLock()
	defer m.RUnlock()

	var failed []Request
	for _, rs := range m.requests {
		for _, r := range rs {
			status := r.Status
			if status == StatusPending && m.expired(r) {
				status = StatusExpired
			}
			if status != StatusPending {
				failed = append(failed, Request{
					Piece:  r.Piece,
					PeerID: r.PeerID,
					Status: status,
				})
			}
		}
	}
	return failed
}

func (m *Manager) validRequest(peerID core.PeerID, i int, allowDuplicates bool) bool {
	for _, r := range m.requests[i] {
		if r.Status == StatusPending && !m.expired(r) {
			if r.PeerID == peerID {
				return false
			}
			if !allowDuplicates {
				return false
			}
		}
	}
	return true
}

func (m *Manager) requestQuota(peerID core.PeerID) int {
	quota := m.pipelineLimit
	pm, ok := m.requestsByPeer[peerID]
	if !ok {
		return quota
	}

	for _, r := range pm {
		if r.Status == StatusPending && !m.expired(r) {
			quota--
			if quota == 0 {
				break
			}
		}
	}

	return quota
}

func (m *Manager) expired(r *Request) bool {
	expiresAt := r.sentAt.Add(m.timeout)
	return m.clock.Now().After(expiresAt)
}

func (m *Manager) markStatus(peerID core.PeerID, i int, s Status) {
	m.Lock()
	defer m.Unlock()

	for _, r := range m.requests[i] {
		if r.PeerID == peerID {
			r.Status = s
		}
	}
}
