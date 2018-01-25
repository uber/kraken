package piecerequest

import (
	"sync"
	"time"

	"github.com/andres-erbsen/clock"

	"code.uber.internal/infra/kraken/torlib"
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
	PeerID torlib.PeerID
	Status Status

	sentAt time.Time
}

// Manager encapsulates thread-safe piece request bookkeeping. It is not responsible
// for sending nor receiving pieces in any way.
type Manager struct {
	sync.Mutex
	requests map[int]*Request
	clock    clock.Clock
	timeout  time.Duration
}

// NewManager creates a new Manager.
func NewManager(clk clock.Clock, timeout time.Duration) *Manager {
	return &Manager{
		requests: make(map[int]*Request),
		clock:    clk,
		timeout:  timeout,
	}
}

// Reserve reserves a piece request for piece i. Returns false if there already
// exists a valid in-flight request for piece i.
func (m *Manager) Reserve(peerID torlib.PeerID, i int) bool {
	m.Lock()
	defer m.Unlock()

	if r, ok := m.requests[i]; ok && r.Status == StatusPending && !m.expired(r) {
		return false
	}
	m.requests[i] = &Request{
		Piece:  i,
		PeerID: peerID,
		Status: StatusPending,
		sentAt: m.clock.Now(),
	}
	return true
}

// MarkUnsent marks the piece request for piece i as unsent.
func (m *Manager) MarkUnsent(peerID torlib.PeerID, i int) {
	m.markStatus(peerID, i, StatusUnsent)
}

// MarkInvalid marks the piece request for piece i as invalid.
func (m *Manager) MarkInvalid(peerID torlib.PeerID, i int) {
	m.markStatus(peerID, i, StatusInvalid)
}

// Clear deletes the piece request for piece i. Should be used for freeing up
// unneeded request bookkeeping.
func (m *Manager) Clear(i int) {
	m.Lock()
	defer m.Unlock()

	delete(m.requests, i)
}

// ClearPeer deletes all piece requests for peerID.
func (m *Manager) ClearPeer(peerID torlib.PeerID) {
	m.Lock()
	defer m.Unlock()

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

func (m *Manager) markStatus(peerID torlib.PeerID, i int, s Status) {
	m.Lock()
	defer m.Unlock()

	if r, ok := m.requests[i]; ok && r.PeerID == peerID {
		r.Status = s
	}
}
