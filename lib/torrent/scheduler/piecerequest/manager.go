package piecerequest

import (
	"math/rand"
	"sync"
	"time"

	"code.uber.internal/infra/kraken/core"

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
	requests       map[int]*Request
	requestsByPeer map[core.PeerID]map[int]*Request

	clock         clock.Clock
	timeout       time.Duration
	pipelineLimit int
}

// NewManager creates a new Manager.
func NewManager(clk clock.Clock, timeout time.Duration, pipelineLimit int) *Manager {
	return &Manager{
		requests:       make(map[int]*Request),
		requestsByPeer: make(map[core.PeerID]map[int]*Request),
		clock:          clk,
		timeout:        timeout,
		pipelineLimit:  pipelineLimit,
	}
}

// ReservePieces selects the next piece(s) to be requested from given peer, and
// save it as pending in request map to avoid duplicate requests.
func (m *Manager) ReservePieces(peerID core.PeerID, candidates *bitset.BitSet) []int {
	m.Lock()
	defer m.Unlock()

	if pm, ok := m.requestsByPeer[peerID]; ok && len(pm) > m.pipelineLimit {
		return nil
	}

	// Reservoir sampling.
	var pieces []int
	count := 0
	for i, e := candidates.NextSet(0); e; i, e = candidates.NextSet(i + 1) {
		if r, ok := m.requests[int(i)]; ok && r.Status == StatusPending && !m.expired(r) {
			continue
		}

		if count < m.pipelineLimit {
			pieces = append(pieces, int(i))
		} else if rand.Intn(count) < 1 {
			// Replace existing result.
			pieces[rand.Intn(m.pipelineLimit)] = int(i)
		}
		count++
	}

	// Set as pending in requests map.
	for _, i := range pieces {
		r := &Request{
			Piece:  i,
			PeerID: peerID,
			Status: StatusPending,
			sentAt: m.clock.Now(),
		}
		m.requests[i] = r
		if _, ok := m.requestsByPeer[peerID]; !ok {
			m.requestsByPeer[peerID] = make(map[int]*Request)
		}
		m.requestsByPeer[peerID][i] = r
	}

	return pieces
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

// ClearPeer deletes all piece requests for peerID.
func (m *Manager) ClearPeer(peerID core.PeerID) {
	m.Lock()
	defer m.Unlock()

	delete(m.requestsByPeer, peerID)

	for i, r := range m.requests {
		if r.PeerID == peerID {
			delete(m.requests, i)
		}
	}
}

// GetFailedRequests returns a copy of all failed piece requests.
func (m *Manager) GetFailedRequests() []Request {
	m.Lock()
	defer m.Unlock()

	var failed []Request
	for _, r := range m.requests {
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
	return failed
}

func (m *Manager) expired(r *Request) bool {
	expiresAt := r.sentAt.Add(m.timeout)
	return m.clock.Now().After(expiresAt)
}

func (m *Manager) markStatus(peerID core.PeerID, i int, s Status) {
	m.Lock()
	defer m.Unlock()

	if r, ok := m.requests[i]; ok && r.PeerID == peerID {
		r.Status = s
	}
}
