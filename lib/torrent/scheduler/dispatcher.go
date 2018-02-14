package scheduler

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"code.uber.internal/infra/kraken/.gen/go/p2p"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/conn"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler/piecerequest"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/timeutil"

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

// messages defines a subset of conn methods which dispatcher requires to
// communicate with remote peers.
type messages interface {
	Send(msg *conn.Message) error
	Receiver() <-chan *conn.Message
	Close()
}

// peer consolidates bookeeping for a remote peer.
type peer struct {
	id core.PeerID

	// Tracks the pieces which the remote peer has.
	bitfield *syncBitfield

	messages messages

	clock clock.Clock

	mu                    sync.Mutex // Protects the following fields:
	lastGoodPieceReceived time.Time
	lastPieceSent         time.Time
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

	p.lastGoodPieceReceived = p.clock.Now()
}

func (p *peer) getLastPieceSent() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.lastPieceSent
}

func (p *peer) touchLastPieceSent() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.lastPieceSent = p.clock.Now()
}

type dispatcherFactory struct {
	Config               DispatcherConfig
	LocalPeerID          core.PeerID
	EventSender          eventSender
	Clock                clock.Clock
	NetworkEventProducer networkevent.Producer
	Stats                tally.Scope
}

func (f *dispatcherFactory) calcPieceRequestTimeout(maxPieceLength int64) time.Duration {
	n := float64(f.Config.PieceRequestTimeoutPerMb) * float64(maxPieceLength) / float64(memsize.MB)
	d := time.Duration(math.Ceil(n))
	return timeutil.MaxDuration(d, f.Config.PieceRequestMinTimeout)
}

// New creates and starts a new dispatcher for the given torrent.
func (f *dispatcherFactory) New(t storage.Torrent) *dispatcher {
	d := f.init(t)

	// Exits when d.pendingPiecesDone is closed.
	go d.watchPendingPieceRequests()

	if t.Complete() {
		d.complete()
	}
	return d
}

func (f *dispatcherFactory) init(t storage.Torrent) *dispatcher {
	pieceRequestTimeout := f.calcPieceRequestTimeout(t.MaxPieceLength())
	pieceRequestManager := piecerequest.NewManager(
		f.Clock, pieceRequestTimeout, f.Config.PipelineLimit)
	return &dispatcher{
		Torrent:             newTorrentAccessWatcher(t, f.Clock),
		CreatedAt:           f.Clock.Now(),
		localPeerID:         f.LocalPeerID,
		eventSender:         f.EventSender,
		clock:               f.Clock,
		networkEvents:       f.NetworkEventProducer,
		pieceRequestManager: pieceRequestManager,
		pieceRequestTimeout: pieceRequestTimeout,
		pendingPiecesDone:   make(chan struct{}),
		stats:               f.Stats,
	}
}

// dispatcher coordinates torrent state with sending / receiving messages between multiple
// peers. As such, dispatcher and torrent have a one-to-one relationship, while dispatcher
// and peer have a one-to-many relationship.
type dispatcher struct {
	Torrent     *torrentAccessWatcher
	CreatedAt   time.Time
	localPeerID core.PeerID
	clock       clock.Clock

	stats tally.Scope

	// Maps core.PeerID to *peer.
	peers syncmap.Map

	eventSender eventSender

	networkEvents networkevent.Producer

	pieceRequestTimeout time.Duration

	pieceRequestManager *piecerequest.Manager

	pendingPiecesDoneOnce sync.Once
	pendingPiecesDone     chan struct{}

	sendCompleteOnce sync.Once
}

func (d *dispatcher) LastGoodPieceReceived(peerID core.PeerID) time.Time {
	v, ok := d.peers.Load(peerID)
	if !ok {
		return time.Time{}
	}
	return v.(*peer).getLastGoodPieceReceived()
}

func (d *dispatcher) LastPieceSent(peerID core.PeerID) time.Time {
	v, ok := d.peers.Load(peerID)
	if !ok {
		return time.Time{}
	}
	return v.(*peer).getLastPieceSent()
}

func (d *dispatcher) LastReadTime() time.Time {
	return d.Torrent.getLastReadTime()
}

func (d *dispatcher) LastWriteTime() time.Time {
	return d.Torrent.getLastWriteTime()
}

// Empty returns true if the dispatcher has no peers.
func (d *dispatcher) Empty() bool {
	// syncmap.Map does not provide a length function, hence this poor man's
	// implementation of `len(d.peers) == 0`.
	empty := true
	d.peers.Range(func(k, v interface{}) bool {
		empty = false
		return false
	})
	return empty
}

// AddPeer registers a new peer with the dispatcher.
func (d *dispatcher) AddPeer(
	peerID core.PeerID, b *bitset.BitSet, messages messages) error {

	p, err := d.addPeer(peerID, b, messages)
	if err != nil {
		return err
	}
	go d.maybeRequestMorePieces(p)
	go d.feed(p)
	return nil
}

// addPeer creates and inserts a new peer into the dispatcher. Split from AddPeer
// with no goroutine side-effects for testing purposes.
func (d *dispatcher) addPeer(
	peerID core.PeerID, b *bitset.BitSet, messages messages) (*peer, error) {

	p := &peer{
		id:       peerID,
		bitfield: newSyncBitfield(b),
		messages: messages,
		clock:    d.clock,
	}
	if _, ok := d.peers.LoadOrStore(peerID, p); ok {
		return nil, errors.New("peer already exists")
	}
	return p, nil
}

// TearDown closes all dispatcher connections.
func (d *dispatcher) TearDown() {
	d.pendingPiecesDoneOnce.Do(func() {
		close(d.pendingPiecesDone)
	})
	d.peers.Range(func(k, v interface{}) bool {
		p := v.(*peer)
		d.log("peer", p).Info("Dispatcher teardown closing connection")
		p.messages.Close()
		return true
	})
}

func (d *dispatcher) String() string {
	return fmt.Sprintf("dispatcher(%s)", d.Torrent)
}

func (d *dispatcher) complete() {
	d.sendCompleteOnce.Do(func() {
		go d.eventSender.Send(completedDispatcherEvent{d})
	})
	d.pendingPiecesDoneOnce.Do(func() {
		close(d.pendingPiecesDone)
	})
	// Close connections to other completed peers since those connections are
	// now useless.
	d.peers.Range(func(k, v interface{}) bool {
		p := v.(*peer)
		if p.bitfield.Complete() {
			d.log("peer", p).Info("Completed dispatcher closing connection to completed peer")
			p.messages.Close()
		}
		return true
	})
}

func (d *dispatcher) maybeRequestMorePieces(p *peer) (bool, error) {
	candidates := p.bitfield.Intersection(d.Torrent.Bitfield().Complement())
	return d.maybeSendPieceRequests(p, candidates)
}

func (d *dispatcher) maybeSendPieceRequests(p *peer, candidates *bitset.BitSet) (bool, error) {
	pieces := d.pieceRequestManager.ReservePieces(p.id, candidates)
	if len(pieces) == 0 {
		return false, nil
	}

	for _, i := range pieces {
		if err := p.messages.Send(conn.NewPieceRequestMessage(i, d.Torrent.PieceLength(i))); err != nil {
			// Connection closed.
			d.pieceRequestManager.MarkUnsent(p.id, i)
			return false, err
		}
		d.networkEvents.Produce(
			networkevent.DispatcherSentPieceRequestEvent(d.Torrent.InfoHash(), d.localPeerID, p.id, i))
	}
	return true, nil
}

func (d *dispatcher) resendFailedPieceRequests() {
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

			b := d.Torrent.Bitfield()
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

func (d *dispatcher) watchPendingPieceRequests() {
	for {
		select {
		case <-d.clock.After(d.pieceRequestTimeout / 2):
			d.resendFailedPieceRequests()
		case <-d.pendingPiecesDone:
			return
		}
	}
}

// feed reads off of peer and handles incoming messages. When peer's messages close,
// the feed goroutine removes peer from the dispatcher and exits.
func (d *dispatcher) feed(p *peer) {
	for msg := range p.messages.Receiver() {
		if err := d.dispatch(p, msg); err != nil {
			d.log().Errorf("Error dispatching message: %s", err)
		}
	}
	d.peers.Delete(p.id)
	d.pieceRequestManager.ClearPeer(p.id)
}

func (d *dispatcher) dispatch(p *peer, msg *conn.Message) error {
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
	default:
		return fmt.Errorf("unknown message type: %d", msg.Message.Type)
	}
	return nil
}

func (d *dispatcher) handleError(p *peer, msg *p2p.ErrorMessage) {
	switch msg.Code {
	case p2p.ErrorMessage_PIECE_REQUEST_FAILED:
		d.log().Errorf("Piece request failed: %s", msg.Error)
		d.pieceRequestManager.MarkInvalid(p.id, int(msg.Index))
	}
}

func (d *dispatcher) handleAnnouncePiece(p *peer, msg *p2p.AnnouncePieceMessage) {
	if int(msg.Index) >= d.Torrent.NumPieces() {
		d.log().Errorf("Announce piece out of bounds: %d >= %d", msg.Index, d.Torrent.NumPieces())
		return
	}
	i := int(msg.Index)
	p.bitfield.Set(uint(i), true)

	d.maybeRequestMorePieces(p)
}

func (d *dispatcher) isFullPiece(i, offset, length int) bool {
	return offset == 0 && length == int(d.Torrent.PieceLength(i))
}

func (d *dispatcher) handlePieceRequest(p *peer, msg *p2p.PieceRequestMessage) {
	i := int(msg.Index)
	if !d.isFullPiece(i, int(msg.Offset), int(msg.Length)) {
		d.log("peer", p, "piece", i).Error("Rejecting piece request: chunk not supported")
		p.messages.Send(conn.NewErrorMessage(i, p2p.ErrorMessage_PIECE_REQUEST_FAILED, errChunkNotSupported))
		return
	}

	d.networkEvents.Produce(
		networkevent.DispatcherGotPieceRequestEvent(d.Torrent.InfoHash(), d.localPeerID, p.id, i))

	payload, err := d.Torrent.GetPieceReader(i)
	if err != nil {
		d.log("peer", p, "piece", i).Errorf("Error getting reader for requested piece: %s", err)
		p.messages.Send(conn.NewErrorMessage(i, p2p.ErrorMessage_PIECE_REQUEST_FAILED, err))
		return
	}

	d.networkEvents.Produce(
		networkevent.DispatcherReadPieceEvent(d.Torrent.InfoHash(), d.localPeerID, p.id, i))

	if err := p.messages.Send(conn.NewPiecePayloadMessage(i, payload)); err != nil {
		return
	}

	d.networkEvents.Produce(
		networkevent.DispatcherSentPiecePayloadEvent(d.Torrent.InfoHash(), d.localPeerID, p.id, i))

	p.touchLastPieceSent()

	// Assume that the peer successfully received the piece.
	p.bitfield.Set(uint(i), true)
}

func (d *dispatcher) handlePiecePayload(
	p *peer, msg *p2p.PiecePayloadMessage, payload storage.PieceReader) {

	defer payload.Close()

	i := int(msg.Index)
	if !d.isFullPiece(i, int(msg.Offset), int(msg.Length)) {
		d.log("peer", p, "piece", i).Error("Rejecting piece payload: chunk not supported")
		d.pieceRequestManager.MarkInvalid(p.id, i)
		return
	}

	d.networkEvents.Produce(
		networkevent.DispatcherGotPiecePayloadEvent(d.Torrent.InfoHash(), d.localPeerID, p.id, i))

	if err := d.Torrent.WritePiece(payload, i); err != nil {
		if err != storage.ErrPieceComplete {
			d.log("peer", p, "piece", i).Errorf("Error writing piece payload: %s", err)
			d.pieceRequestManager.MarkInvalid(p.id, i)
		}
		return
	}

	d.networkEvents.Produce(
		networkevent.DispatcherWrotePieceEvent(d.Torrent.InfoHash(), d.localPeerID, p.id, i))

	d.networkEvents.Produce(
		networkevent.ReceivePieceEvent(d.Torrent.InfoHash(), d.localPeerID, p.id, i))

	p.touchLastGoodPieceReceived()
	if d.Torrent.Complete() {
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

func (d *dispatcher) handleCancelPiece(p *peer, msg *p2p.CancelPieceMessage) {
	// No-op: cancelling not supported because all received messages are synchronized,
	// therefore if we receive a cancel it is already too late -- we've already read
	// the piece.
}

func (d *dispatcher) handleBitfield(p *peer, msg *p2p.BitfieldMessage) {
	d.log("peer", p).Error("Unexpected bitfield message from established conn")
}

func (d *dispatcher) log(args ...interface{}) *zap.SugaredLogger {
	args = append(args, "torrent", d.Torrent)
	return log.With(args...)
}
