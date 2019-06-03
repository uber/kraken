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
package dispatch

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/gen/go/proto/p2p"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/scheduler/conn"
	"github.com/uber/kraken/lib/torrent/scheduler/dispatch/piecerequest"
	"github.com/uber/kraken/lib/torrent/scheduler/torrentlog"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/utils/syncutil"

	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
	"github.com/willf/bitset"
	"go.uber.org/zap"
	"golang.org/x/sync/syncmap"
)

var (
	errPeerAlreadyDispatched   = errors.New("peer is already dispatched for the torrent")
	errPieceOutOfBounds        = errors.New("piece index out of bounds")
	errChunkNotSupported       = errors.New("reading / writing chunk of piece not supported")
	errRepeatedBitfieldMessage = errors.New("received repeated bitfield message")
)

// Events defines Dispatcher events.
type Events interface {
	DispatcherComplete(*Dispatcher)
	PeerRemoved(core.PeerID, core.InfoHash)
}

// Messages defines a subset of conn.Conn methods which Dispatcher requires to
// communicate with remote peers.
type Messages interface {
	Send(msg *conn.Message) error
	Receiver() <-chan *conn.Message
	Close()
}

// Dispatcher coordinates torrent state with sending / receiving messages between multiple
// peers. As such, Dispatcher and Torrent have a one-to-one relationship, while Dispatcher
// and Conn have a one-to-many relationship.
type Dispatcher struct {
	config                Config
	stats                 tally.Scope
	clk                   clock.Clock
	createdAt             time.Time
	localPeerID           core.PeerID
	torrent               *torrentAccessWatcher
	peers                 syncmap.Map // core.PeerID -> *peer
	peerStats             syncmap.Map // core.PeerID -> *peerStats, persists on peer removal.
	numPeersByPiece       syncutil.Counters
	netevents             networkevent.Producer
	pieceRequestTimeout   time.Duration
	pieceRequestManager   *piecerequest.Manager
	pendingPiecesDoneOnce sync.Once
	pendingPiecesDone     chan struct{}
	completeOnce          sync.Once
	events                Events
	logger                *zap.SugaredLogger
	torrentlog            *torrentlog.Logger
}

// New creates a new Dispatcher.
func New(
	config Config,
	stats tally.Scope,
	clk clock.Clock,
	netevents networkevent.Producer,
	events Events,
	peerID core.PeerID,
	t storage.Torrent,
	logger *zap.SugaredLogger,
	tlog *torrentlog.Logger) (*Dispatcher, error) {

	d, err := newDispatcher(config, stats, clk, netevents, events, peerID, t, logger, tlog)
	if err != nil {
		return nil, err
	}

	// Exits when d.pendingPiecesDone is closed.
	go d.watchPendingPieceRequests()

	if t.Complete() {
		d.complete()
	}

	return d, nil
}

// newDispatcher creates a new Dispatcher with no side-effects for testing purposes.
func newDispatcher(
	config Config,
	stats tally.Scope,
	clk clock.Clock,
	netevents networkevent.Producer,
	events Events,
	peerID core.PeerID,
	t storage.Torrent,
	logger *zap.SugaredLogger,
	tlog *torrentlog.Logger) (*Dispatcher, error) {

	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "dispatch",
	})

	pieceRequestTimeout := config.calcPieceRequestTimeout(t.MaxPieceLength())
	pieceRequestManager, err := piecerequest.NewManager(
		clk, pieceRequestTimeout, config.PieceRequestPolicy, config.PipelineLimit)
	if err != nil {
		return nil, fmt.Errorf("piece request manager: %s", err)
	}

	return &Dispatcher{
		config:              config,
		stats:               stats,
		clk:                 clk,
		createdAt:           clk.Now(),
		localPeerID:         peerID,
		torrent:             newTorrentAccessWatcher(t, clk),
		numPeersByPiece:     syncutil.NewCounters(t.NumPieces()),
		netevents:           netevents,
		pieceRequestTimeout: pieceRequestTimeout,
		pieceRequestManager: pieceRequestManager,
		pendingPiecesDone:   make(chan struct{}),
		events:              events,
		logger:              logger,
		torrentlog:          tlog,
	}, nil
}

// Digest returns the blob digest for d's torrent.
func (d *Dispatcher) Digest() core.Digest {
	return d.torrent.Digest()
}

// InfoHash returns d's torrent hash.
func (d *Dispatcher) InfoHash() core.InfoHash {
	return d.torrent.InfoHash()
}

// Length returns d's torrent length.
func (d *Dispatcher) Length() int64 {
	return d.torrent.Length()
}

// Stat returns d's TorrentInfo.
func (d *Dispatcher) Stat() *storage.TorrentInfo {
	return d.torrent.Stat()
}

// Complete returns true if d's torrent is complete.
func (d *Dispatcher) Complete() bool {
	return d.torrent.Complete()
}

// CreatedAt returns when d was created.
func (d *Dispatcher) CreatedAt() time.Time {
	return d.createdAt
}

// LastGoodPieceReceived returns when d last received a valid and needed piece
// from peerID.
func (d *Dispatcher) LastGoodPieceReceived(peerID core.PeerID) time.Time {
	v, ok := d.peers.Load(peerID)
	if !ok {
		return time.Time{}
	}
	return v.(*peer).getLastGoodPieceReceived()
}

// LastPieceSent returns when d last sent a piece to peerID.
func (d *Dispatcher) LastPieceSent(peerID core.PeerID) time.Time {
	v, ok := d.peers.Load(peerID)
	if !ok {
		return time.Time{}
	}
	return v.(*peer).getLastPieceSent()
}

// LastReadTime returns when d's torrent was last read from.
func (d *Dispatcher) LastReadTime() time.Time {
	return d.torrent.getLastReadTime()
}

// LastWriteTime returns when d's torrent was last written to.
func (d *Dispatcher) LastWriteTime() time.Time {
	return d.torrent.getLastWriteTime()
}

// Empty returns true if the Dispatcher has no peers.
func (d *Dispatcher) Empty() bool {
	empty := true
	d.peers.Range(func(k, v interface{}) bool {
		empty = false
		return false
	})
	return empty
}

// RemoteBitfields returns the bitfields of peers connected to the dispatcher.
func (d *Dispatcher) RemoteBitfields() conn.RemoteBitfields {
	remoteBitfields := make(conn.RemoteBitfields)

	d.peers.Range(func(k, v interface{}) bool {
		remoteBitfields[k.(core.PeerID)] = v.(*peer).bitfield.Copy()
		return true
	})
	return remoteBitfields
}

// AddPeer registers a new peer with the Dispatcher.
func (d *Dispatcher) AddPeer(
	peerID core.PeerID, b *bitset.BitSet, messages Messages) error {

	p, err := d.addPeer(peerID, b, messages)
	if err != nil {
		return err
	}
	go d.maybeRequestMorePieces(p)
	go d.feed(p)
	return nil
}

// addPeer creates and inserts a new peer into the Dispatcher. Split from AddPeer
// with no goroutine side-effects for testing purposes.
func (d *Dispatcher) addPeer(
	peerID core.PeerID, b *bitset.BitSet, messages Messages) (*peer, error) {

	pstats := &peerStats{}
	if s, ok := d.peerStats.LoadOrStore(peerID, pstats); ok {
		pstats = s.(*peerStats)
	}

	p := newPeer(peerID, b, messages, d.clk, pstats)
	if _, ok := d.peers.LoadOrStore(peerID, p); ok {
		return nil, errors.New("peer already exists")
	}

	for _, i := range p.bitfield.GetAllSet() {
		d.numPeersByPiece.Increment(int(i))
	}
	return p, nil
}

func (d *Dispatcher) removePeer(p *peer) error {
	d.peers.Delete(p.id)
	d.pieceRequestManager.ClearPeer(p.id)

	for _, i := range p.bitfield.GetAllSet() {
		d.numPeersByPiece.Decrement(int(i))
	}
	return nil
}

// TearDown closes all Dispatcher connections.
func (d *Dispatcher) TearDown() {
	d.pendingPiecesDoneOnce.Do(func() {
		close(d.pendingPiecesDone)
	})

	d.peers.Range(func(k, v interface{}) bool {
		p := v.(*peer)
		d.log("peer", p).Info("Dispatcher teardown closing connection")
		p.messages.Close()
		return true
	})

	summaries := make(torrentlog.LeecherSummaries, 0)
	d.peerStats.Range(func(k, v interface{}) bool {
		peerID := k.(core.PeerID)
		pstats := v.(*peerStats)
		summaries = append(summaries, torrentlog.LeecherSummary{
			PeerID:           peerID,
			RequestsReceived: pstats.getPieceRequestsReceived(),
			PiecesSent:       pstats.getPiecesSent(),
		})
		return true
	})

	if err := d.torrentlog.LeecherSummaries(
		d.torrent.Digest(), d.torrent.InfoHash(), summaries); err != nil {
		d.log().Errorf("Error logging incoming piece request summary: %s", err)
	}
}

func (d *Dispatcher) String() string {
	return fmt.Sprintf("Dispatcher(%s)", d.torrent)
}

func (d *Dispatcher) complete() {
	d.completeOnce.Do(func() { go d.events.DispatcherComplete(d) })
	d.pendingPiecesDoneOnce.Do(func() { close(d.pendingPiecesDone) })

	d.peers.Range(func(k, v interface{}) bool {
		p := v.(*peer)
		if p.bitfield.Complete() {
			// Close connections to other completed peers since those connections
			// are now useless.
			d.log("peer", p).Info("Closing connection to completed peer")
			p.messages.Close()
		} else {
			// Notify in-progress peers that we have completed the torrent and
			// all pieces are available.
			p.messages.Send(conn.NewCompleteMessage())
		}
		return true
	})

	var piecesRequestedTotal int
	summaries := make(torrentlog.SeederSummaries, 0)
	d.peerStats.Range(func(k, v interface{}) bool {
		peerID := k.(core.PeerID)
		pstats := v.(*peerStats)
		requested := pstats.getPieceRequestsSent()
		piecesRequestedTotal += requested
		summary := torrentlog.SeederSummary{
			PeerID:         peerID,
			RequestsSent:   requested,
			GoodPiecesReceived: pstats.getGoodPiecesReceived(),
			DuplicatePiecesReceived: pstats.getDuplicatePiecesReceived(),
		}
		summaries = append(summaries, summary)
		return true
	})

	// Only log if we actually requested pieces from others.
	if piecesRequestedTotal > 0 {
		if err := d.torrentlog.SeederSummaries(
			d.torrent.Digest(), d.torrent.InfoHash(), summaries); err != nil {
			d.log().Errorf("Error logging outgoing piece request summary: %s", err)
		}
	}
}

func (d *Dispatcher) endgame() bool {
	if d.config.DisableEndgame {
		return false
	}
	remaining := d.torrent.NumPieces() - int(d.torrent.Bitfield().Count())
	return remaining <= d.config.EndgameThreshold
}

func (d *Dispatcher) maybeRequestMorePieces(p *peer) (bool, error) {
	candidates := p.bitfield.Intersection(d.torrent.Bitfield().Complement())

	return d.maybeSendPieceRequests(p, candidates)
}

func (d *Dispatcher) maybeSendPieceRequests(p *peer, candidates *bitset.BitSet) (bool, error) {
	pieces, err := d.pieceRequestManager.ReservePieces(p.id, candidates, d.numPeersByPiece, d.endgame())
	if err != nil {
		return false, err
	}
	if len(pieces) == 0 {
		return false, nil
	}
	for _, i := range pieces {
		if err := p.messages.Send(conn.NewPieceRequestMessage(i, d.torrent.PieceLength(i))); err != nil {
			// Connection closed.
			d.pieceRequestManager.MarkUnsent(p.id, i)
			return false, err
		}
		d.netevents.Produce(
			networkevent.RequestPieceEvent(d.torrent.InfoHash(), d.localPeerID, p.id, i))
			p.pstats.incrementPieceRequestsSent()
		}
	return true, nil
}

func (d *Dispatcher) resendFailedPieceRequests() {
	failedRequests := d.pieceRequestManager.GetFailedRequests()
	if len(failedRequests) > 0 {
		d.log().Infof("Resending %d failed piece requests", len(failedRequests))
		d.stats.Counter("piece_request_failures").Inc(int64(len(failedRequests)))
	}

	var sent int
	for _, r := range failedRequests {
		d.peers.Range(func(k, v interface{}) bool {
			p := v.(*peer)
			if (r.Status == piecerequest.StatusExpired || r.Status == piecerequest.StatusInvalid) &&
				r.PeerID == p.id {
				// Do not resend to the same peer for expired or invalid requests.
				return true
			}

			b := d.torrent.Bitfield()
			candidates := p.bitfield.Intersection(b.Complement())
			if candidates.Test(uint(r.Piece)) {
				nb := bitset.New(b.Len()).Set(uint(r.Piece))
				if sent, err := d.maybeSendPieceRequests(p, nb); sent && err == nil {
					return false
				}
			}
			return true
		})
	}

	unsent := len(failedRequests) - sent
	if unsent > 0 {
		d.log().Infof("Nowhere to resend %d / %d failed piece requests", unsent, len(failedRequests))
	}
}

func (d *Dispatcher) watchPendingPieceRequests() {
	for {
		select {
		case <-d.clk.After(d.pieceRequestTimeout / 2):
			d.resendFailedPieceRequests()
		case <-d.pendingPiecesDone:
			return
		}
	}
}

// feed reads off of peer and handles incoming messages. When peer's messages close,
// the feed goroutine removes peer from the Dispatcher and exits.
func (d *Dispatcher) feed(p *peer) {
	for msg := range p.messages.Receiver() {
		if err := d.dispatch(p, msg); err != nil {
			d.log().Errorf("Error dispatching message: %s", err)
		}
	}
	d.removePeer(p)
	d.events.PeerRemoved(p.id, d.torrent.InfoHash())
}

func (d *Dispatcher) dispatch(p *peer, msg *conn.Message) error {
	switch msg.Message.Type {
	case p2p.Message_ERROR:
		d.handleError(p, msg.Message.Error)
	case p2p.Message_ANNOUCE_PIECE:
		d.handleAnnouncePiece(p, msg.Message.AnnouncePiece)
	case p2p.Message_PIECE_REQUEST:
		d.handlePieceRequest(p, msg.Message.PieceRequest)
	case p2p.Message_PIECE_PAYLOAD:
		d.handlePiecePayload(p, msg.Message.PiecePayload, msg.Payload)
	case p2p.Message_CANCEL_PIECE:
		d.handleCancelPiece(p, msg.Message.CancelPiece)
	case p2p.Message_BITFIELD:
		d.handleBitfield(p, msg.Message.Bitfield)
	case p2p.Message_COMPLETE:
		d.handleComplete(p)
	default:
		return fmt.Errorf("unknown message type: %d", msg.Message.Type)
	}
	return nil
}

func (d *Dispatcher) handleError(p *peer, msg *p2p.ErrorMessage) {
	switch msg.Code {
	case p2p.ErrorMessage_PIECE_REQUEST_FAILED:
		d.log().Errorf("Piece request failed: %s", msg.Error)
		d.pieceRequestManager.MarkInvalid(p.id, int(msg.Index))
	}
}

func (d *Dispatcher) handleAnnouncePiece(p *peer, msg *p2p.AnnouncePieceMessage) {
	if int(msg.Index) >= d.torrent.NumPieces() {
		d.log().Errorf("Announce piece out of bounds: %d >= %d", msg.Index, d.torrent.NumPieces())
		return
	}
	i := int(msg.Index)
	p.bitfield.Set(uint(i), true)
	d.numPeersByPiece.Increment(int(i))

	d.maybeRequestMorePieces(p)
}

func (d *Dispatcher) isFullPiece(i, offset, length int) bool {
	return offset == 0 && length == int(d.torrent.PieceLength(i))
}

func (d *Dispatcher) handlePieceRequest(p *peer, msg *p2p.PieceRequestMessage) {
	p.pstats.incrementPieceRequestsReceived()

	i := int(msg.Index)
	if !d.isFullPiece(i, int(msg.Offset), int(msg.Length)) {
		d.log("peer", p, "piece", i).Error("Rejecting piece request: chunk not supported")
		p.messages.Send(conn.NewErrorMessage(i, p2p.ErrorMessage_PIECE_REQUEST_FAILED, errChunkNotSupported))
		return
	}

	payload, err := d.torrent.GetPieceReader(i)
	if err != nil {
		d.log("peer", p, "piece", i).Errorf("Error getting reader for requested piece: %s", err)
		p.messages.Send(conn.NewErrorMessage(i, p2p.ErrorMessage_PIECE_REQUEST_FAILED, err))
		return
	}

	if err := p.messages.Send(conn.NewPiecePayloadMessage(i, payload)); err != nil {
		return
	}

	p.touchLastPieceSent()
	p.pstats.incrementPiecesSent()

	// Assume that the peer successfully received the piece.
	p.bitfield.Set(uint(i), true)
}

func (d *Dispatcher) handlePiecePayload(
	p *peer, msg *p2p.PiecePayloadMessage, payload storage.PieceReader) {

	defer payload.Close()

	i := int(msg.Index)
	if !d.isFullPiece(i, int(msg.Offset), int(msg.Length)) {
		d.log("peer", p, "piece", i).Error("Rejecting piece payload: chunk not supported")
		d.pieceRequestManager.MarkInvalid(p.id, i)
		return
	}

	if err := d.torrent.WritePiece(payload, i); err != nil {
		if err != storage.ErrPieceComplete {
			d.log("peer", p, "piece", i).Errorf("Error writing piece payload: %s", err)
			d.pieceRequestManager.MarkInvalid(p.id, i)
		} else {
			p.pstats.incrementDuplicatePiecesReceived()
		}
		return
	}

	d.netevents.Produce(
		networkevent.ReceivePieceEvent(d.torrent.InfoHash(), d.localPeerID, p.id, i))

	p.pstats.incrementGoodPiecesReceived()
	p.touchLastGoodPieceReceived()
	if d.torrent.Complete() {
		d.complete()
	}

	d.pieceRequestManager.Clear(i)

	d.maybeRequestMorePieces(p)

	d.peers.Range(func(k, v interface{}) bool {
		if k.(core.PeerID) == p.id {
			return true
		}
		pp := v.(*peer)

		pp.messages.Send(conn.NewAnnouncePieceMessage(i))

		return true
	})
}

func (d *Dispatcher) handleCancelPiece(p *peer, msg *p2p.CancelPieceMessage) {
	// No-op: cancelling not supported because all received messages are synchronized,
	// therefore if we receive a cancel it is already too late -- we've already read
	// the piece.
}

func (d *Dispatcher) handleBitfield(p *peer, msg *p2p.BitfieldMessage) {
	d.log("peer", p).Error("Unexpected bitfield message from established conn")
}

func (d *Dispatcher) handleComplete(p *peer) {
	if d.Complete() {
		d.log("peer", p).Info("Closing connection to completed peer")
		p.messages.Close()
	} else {
		p.bitfield.SetAll(true)
		d.maybeRequestMorePieces(p)
	}
}

func (d *Dispatcher) log(args ...interface{}) *zap.SugaredLogger {
	args = append(args, "torrent", d.torrent)
	return d.logger.With(args...)
}
