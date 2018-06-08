package dispatch

import (
	"sync"
	"time"

	"code.uber.internal/infra/kraken/core"
	"github.com/andres-erbsen/clock"
	"github.com/willf/bitset"
)

// peer consolidates bookeeping for a remote peer.
type peer struct {
	id core.PeerID

	// Tracks the pieces which the remote peer has.
	bitfield *syncBitfield

	messages Messages

	clk clock.Clock

	// May be accessed outside of the peer struct.
	pstats *peerStats

	mu                    sync.Mutex // Protects the following fields:
	lastGoodPieceReceived time.Time
	lastPieceSent         time.Time
}

func newPeer(
	peerID core.PeerID,
	b *bitset.BitSet,
	messages Messages,
	clk clock.Clock,
	pstats *peerStats) *peer {

	return &peer{
		id:       peerID,
		bitfield: newSyncBitfield(b),
		messages: messages,
		clk:      clk,
		pstats:   pstats,
	}
}

func (p *peer) String() string {
	return p.id.String()
}

func (p *peer) getLastGoodPieceReceived() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.lastGoodPieceReceived
}

func (p *peer) touchLastGoodPieceReceived() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lastGoodPieceReceived = p.clk.Now()
}

func (p *peer) getLastPieceSent() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.lastPieceSent
}

func (p *peer) touchLastPieceSent() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lastPieceSent = p.clk.Now()
}

// peerStats wraps stats collected for a given peer.
type peerStats struct {
	mu                    sync.Mutex
	pieceRequestsSent     int // pieces we requested from the peer
	pieceRequestsReceived int // pieces the peer requested from us
	piecesSent            int // pieces we sent to the peer
	piecesReceived        int // pieces we received from the peer
}

func (s *peerStats) getPieceRequestsSent() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pieceRequestsSent
}

func (s *peerStats) incrementPieceRequestsSent() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pieceRequestsSent++
}

func (s *peerStats) getPieceRequestsReceived() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pieceRequestsReceived
}

func (s *peerStats) incrementPieceRequestsReceived() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pieceRequestsReceived++
}

func (s *peerStats) getPiecesSent() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.piecesSent
}

func (s *peerStats) incrementPiecesSent() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.piecesSent++
}

func (s *peerStats) getPiecesReceived() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.piecesReceived
}

func (s *peerStats) incrementPiecesReceived() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.piecesReceived++
}
