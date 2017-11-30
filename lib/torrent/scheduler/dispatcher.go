package scheduler

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"golang.org/x/sync/syncmap"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/.gen/go/p2p"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/timeutil"
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
	Send(msg *message) error
	Receiver() <-chan *message
	Close()
}

// peer consolidates bookeeping for a remote peer.
type peer struct {
	id torlib.PeerID

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

type pendingPieceRequest struct {
	i      int
	peerID torlib.PeerID
	sentAt time.Time
}

type dispatcherFactory struct {
	Config               DispatcherConfig
	LocalPeerID          torlib.PeerID
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
	return &dispatcher{
		Torrent:              t,
		CreatedAt:            f.Clock.Now(),
		localPeerID:          f.LocalPeerID,
		eventSender:          f.EventSender,
		clock:                f.Clock,
		networkEventProducer: f.NetworkEventProducer,
		pieceRequestTimeout:  f.calcPieceRequestTimeout(t.MaxPieceLength()),
		pendingPieceRequests: make(map[int]pendingPieceRequest),
		pendingPiecesDone:    make(chan struct{}),
		stats:                f.Stats,
	}
}

// dispatcher coordinates torrent state with sending / receiving messages between multiple
// peers. As such, dispatcher and torrent have a one-to-one relationship, while dispatcher
// and peer have a one-to-many relationship.
type dispatcher struct {
	Torrent     storage.Torrent
	CreatedAt   time.Time
	localPeerID torlib.PeerID
	clock       clock.Clock

	stats tally.Scope

	// Maps torlib.PeerID to *peer.
	peers syncmap.Map

	eventSender eventSender

	networkEventProducer networkevent.Producer

	lastConnRemovedMu sync.Mutex
	lastConnRemoved   time.Time

	pieceRequestTimeout time.Duration

	pendingPieceRequestsMu sync.Mutex
	pendingPieceRequests   map[int]pendingPieceRequest

	pendingPiecesDoneOnce sync.Once
	pendingPiecesDone     chan struct{}

	sendCompleteOnce sync.Once
}

func (d *dispatcher) LastGoodPieceReceived(peerID torlib.PeerID) time.Time {
	v, ok := d.peers.Load(peerID)
	if !ok {
		return time.Time{}
	}
	return v.(*peer).getLastGoodPieceReceived()
}

func (d *dispatcher) LastPieceSent(peerID torlib.PeerID) time.Time {
	v, ok := d.peers.Load(peerID)
	if !ok {
		return time.Time{}
	}
	return v.(*peer).getLastPieceSent()
}

func (d *dispatcher) LastConnRemoved() time.Time {
	d.lastConnRemovedMu.Lock()
	defer d.lastConnRemovedMu.Unlock()

	return d.lastConnRemoved
}

func (d *dispatcher) touchLastConnRemoved() {
	d.lastConnRemovedMu.Lock()
	defer d.lastConnRemovedMu.Unlock()

	d.lastConnRemoved = d.clock.Now()
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
	peerID torlib.PeerID, b storage.Bitfield, messages messages) error {

	p, err := d.addPeer(peerID, b, messages)
	if err != nil {
		return err
	}
	go d.sendInitialPieceRequests(p)
	go d.feed(p)
	return nil
}

// addPeer creates and inserts a new peer into the dispatcher. Split from AddPeer
// with no goroutine side-effects for testing purposes.
func (d *dispatcher) addPeer(
	peerID torlib.PeerID, b storage.Bitfield, messages messages) (*peer, error) {

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
}

func (d *dispatcher) expiredPieceRequest(r pendingPieceRequest) bool {
	expiresAt := r.sentAt.Add(d.pieceRequestTimeout)
	return d.clock.Now().After(expiresAt)
}

func (d *dispatcher) clearPieceRequest(peerID torlib.PeerID, i int) {
	d.pendingPieceRequestsMu.Lock()
	defer d.pendingPieceRequestsMu.Unlock()

	if r, ok := d.pendingPieceRequests[i]; ok && r.peerID == peerID {
		delete(d.pendingPieceRequests, i)
	}
}

func (d *dispatcher) reservePieceRequest(peerID torlib.PeerID, i int) bool {
	d.pendingPieceRequestsMu.Lock()
	defer d.pendingPieceRequestsMu.Unlock()

	if r, ok := d.pendingPieceRequests[i]; ok && !d.expiredPieceRequest(r) {
		return false
	}
	d.pendingPieceRequests[i] = pendingPieceRequest{i, peerID, d.clock.Now()}
	return true
}

func (d *dispatcher) getExpiredPieceRequests() []pendingPieceRequest {
	d.pendingPieceRequestsMu.Lock()
	defer d.pendingPieceRequestsMu.Unlock()

	var expired []pendingPieceRequest
	for _, r := range d.pendingPieceRequests {
		if d.expiredPieceRequest(r) {
			expired = append(expired, r)
		}
	}
	return expired
}

func (d *dispatcher) maybeSendPieceRequest(p *peer, i int) error {
	if d.Torrent.HasPiece(i) {
		// No-op: we already have this piece.
		return nil
	}
	if !d.reservePieceRequest(p.id, i) {
		// No-op: we have already have a non-expired pending request for this piece.
		return nil
	}
	if err := p.messages.Send(newPieceRequestMessage(i, d.Torrent.PieceLength(i))); err != nil {
		d.clearPieceRequest(p.id, i)
		return err
	}
	return nil
}

func (d *dispatcher) resendExpiredPieceRequests() {
	expired := d.getExpiredPieceRequests()
	if len(expired) > 0 {
		d.log().Infof("Resending %d expired piece requests", len(expired))
		d.stats.Counter("piece_request_timeouts").Inc(int64(len(expired)))
	}
	var sent int
	for _, r := range expired {
		d.peers.Range(func(k, v interface{}) bool {
			p := v.(*peer)
			if p.id == r.peerID {
				// The expired request is from this peer -- do not request again.
				return true
			}
			if p.bitfield.Has(r.i) {
				if err := d.maybeSendPieceRequest(p, r.i); err == nil {
					sent++
					return false
				}
			}
			return true
		})
		// NOTE: It is possible that we were unable to resend the piece
		// request. This is fine -- future piece announcements / handshakes
		// will still trigger piece requests.
	}
	failures := len(expired) - sent
	if failures > 0 {
		d.log().Infof("Nowhere to resend %d / %d expired piece requests", failures, len(expired))
	}
}

func (d *dispatcher) watchPendingPieceRequests() {
	for {
		select {
		case <-d.clock.After(d.pieceRequestTimeout / 2):
			d.resendExpiredPieceRequests()
		case <-d.pendingPiecesDone:
			return
		}
	}
}

func (d *dispatcher) sendInitialPieceRequests(p *peer) {
	for _, i := range d.Torrent.MissingPieces() {
		if !p.bitfield.Has(i) {
			continue
		}
		if err := d.maybeSendPieceRequest(p, i); err != nil {
			// Connection closed.
			break
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
	d.touchLastConnRemoved()
}

func (d *dispatcher) dispatch(p *peer, msg *message) error {
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
		d.clearPieceRequest(p.id, int(msg.Index))
	}
}

func (d *dispatcher) handleAnnouncePiece(p *peer, msg *p2p.AnnouncePieceMessage) {
	if int(msg.Index) >= d.Torrent.NumPieces() {
		d.log().Errorf("Announce piece out of bounds: %d >= %d", msg.Index, d.Torrent.NumPieces())
		return
	}
	i := int(msg.Index)
	p.bitfield.Set(i, true)
	d.maybeSendPieceRequest(p, i)
}

func (d *dispatcher) isFullPiece(i, offset, length int) bool {
	return offset == 0 && length == int(d.Torrent.PieceLength(i))
}

func (d *dispatcher) handlePieceRequest(p *peer, msg *p2p.PieceRequestMessage) {
	i := int(msg.Index)
	if !d.isFullPiece(i, int(msg.Offset), int(msg.Length)) {
		d.logf(log.Fields{"peer": p, "piece": i}).Error("Rejecting piece request: chunk not supported")
		p.messages.Send(newErrorMessage(i, p2p.ErrorMessage_PIECE_REQUEST_FAILED, errChunkNotSupported))
		return
	}

	payload, err := d.Torrent.ReadPiece(i)
	if err != nil {
		d.logf(log.Fields{"peer": p, "piece": i}).Errorf("Error reading requested piece: %s", err)
		p.messages.Send(newErrorMessage(i, p2p.ErrorMessage_PIECE_REQUEST_FAILED, err))
		return
	}
	if err := p.messages.Send(newPiecePayloadMessage(i, payload)); err != nil {
		d.logf(log.Fields{"peer": p, "piece": i}).Errorf("Failed to send piece: %s", err)
		return
	}
	p.touchLastPieceSent()
}

func (d *dispatcher) handlePiecePayload(
	p *peer, msg *p2p.PiecePayloadMessage, payload []byte) {

	i := int(msg.Index)
	if !d.isFullPiece(i, int(msg.Offset), int(msg.Length)) {
		d.logf(log.Fields{"peer": p, "piece": i}).Error("Rejecting piece payload: chunk not supported")
		d.clearPieceRequest(p.id, i)
		return
	}
	if err := d.Torrent.WritePiece(payload, i); err != nil {
		if err != storage.ErrPieceComplete {
			d.logf(log.Fields{"peer": p, "piece": i}).Errorf("Error writing piece payload: %s", err)
			d.clearPieceRequest(p.id, i)
		}
		return
	}
	d.networkEventProducer.Produce(
		networkevent.ReceivePieceEvent(d.Torrent.InfoHash(), d.localPeerID, p.id, i))
	p.touchLastGoodPieceReceived()
	if d.Torrent.Complete() {
		d.complete()
	}

	d.peers.Range(func(k, v interface{}) bool {
		if k.(torlib.PeerID) == p.id {
			return true
		}
		pp := v.(*peer)

		// Ignore error -- this just means the connection was closed. The feed goroutine
		// for pp will clean up.
		pp.messages.Send(newAnnouncePieceMessage(i))

		return true
	})
}

func (d *dispatcher) handleCancelPiece(p *peer, msg *p2p.CancelPieceMessage) {
	// No-op: cancelling not supported because all received messages are synchronized,
	// therefore if we receive a cancel it is already too late -- we've already read
	// the piece.
}

func (d *dispatcher) handleBitfield(p *peer, msg *p2p.BitfieldMessage) {
	log.WithFields(log.Fields{"peer": p}).Error("Unexpected bitfield message from established conn")
}

func (d *dispatcher) logf(f log.Fields) bark.Logger {
	f["torrent"] = d.Torrent
	return log.WithFields(f)
}

func (d *dispatcher) log() bark.Logger {
	return d.logf(log.Fields{})
}
