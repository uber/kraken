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

	mu                    sync.Mutex // Protects the following fields:
	lastGoodPieceReceived time.Time
	lastPieceSent         time.Time
}

func newPeer(peerID core.PeerID, b *bitset.BitSet, messages Messages, clk clock.Clock) *peer {
	return &peer{
		id:       peerID,
		bitfield: newSyncBitfield(b),
		messages: messages,
		clk:      clk,
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
