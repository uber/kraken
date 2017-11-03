package scheduler

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber-common/bark"
	"golang.org/x/sync/syncmap"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/.gen/go/p2p"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
)

var (
	errPeerAlreadyDispatched   = errors.New("peer is already dispatched for the torrent")
	errPieceOutOfBounds        = errors.New("piece index out of bounds")
	errChunkNotSupported       = errors.New("reading / writing chunk of piece not supported")
	errRepeatedBitfieldMessage = errors.New("received repeated bitfield message")
)

type dispatcherFactory struct {
	Config               Config
	LocalPeerID          torlib.PeerID
	EventSender          eventSender
	Clock                clock.Clock
	NetworkEventProducer networkevent.Producer
}

// New creates a new dispatcher for the given torrent.
func (f *dispatcherFactory) New(t storage.Torrent) *dispatcher {
	d := &dispatcher{
		Torrent:              t,
		CreatedAt:            f.Clock.Now(),
		localPeerID:          f.LocalPeerID,
		eventSender:          f.EventSender,
		clock:                f.Clock,
		networkEventProducer: f.NetworkEventProducer,
	}
	if t.Complete() {
		d.complete.Do(func() { go d.eventSender.Send(completedDispatcherEvent{d}) })
	}
	return d
}

// dispatcher coordinates torrent state with sending / receiving messages between multiple
// peers. As such, dispatcher and torrent have a one-to-one relationship, while dispatcher
// and conn have a many-to-many relationship.
type dispatcher struct {
	Torrent     storage.Torrent
	CreatedAt   time.Time
	localPeerID torlib.PeerID
	clock       clock.Clock

	// Maps torlib.PeerID to *conn.
	conns syncmap.Map

	eventSender eventSender

	networkEventProducer networkevent.Producer

	mu              sync.Mutex // Protects the following fields:
	lastConnRemoved time.Time

	// complete ensures dispatcher only sends complete event once to scheduler
	complete sync.Once
}

func (d *dispatcher) LastConnRemoved() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.lastConnRemoved
}

func (d *dispatcher) touchLastConnRemoved() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.lastConnRemoved = d.clock.Now()
}

// Empty returns true if the dispatcher has no conns.
func (d *dispatcher) Empty() bool {
	// syncmap.Map does not provide a length function, hence this poor man's
	// implementation of `len(d.conns) == 0`.
	empty := true
	d.conns.Range(func(k, v interface{}) bool {
		empty = false
		return false
	})
	return empty
}

// AddConn registers a new conn with the dispatcher.
func (d *dispatcher) AddConn(c *conn) error {
	if c.InfoHash != d.Torrent.InfoHash() {
		return errors.New("conn initialized for wrong torrent")
	}
	if _, ok := d.conns.LoadOrStore(c.PeerID, c); ok {
		return errors.New("conn already exists")
	}
	go d.sendInitialPieceRequests(c)
	go d.feed(c)
	return nil
}

// TearDown closes all dispatcher connections.
func (d *dispatcher) TearDown() {
	d.conns.Range(func(k, v interface{}) bool {
		conn := v.(*conn)
		conn.Close()
		return true
	})
}

func (d *dispatcher) String() string {
	return fmt.Sprintf("dispatcher(%s)", d.Torrent)
}

func (d *dispatcher) sendPieceRequest(c *conn, i int) error {
	d.logf(log.Fields{"conn": c, "piece": i}).Info("Sending piece request")
	return c.Send(newPieceRequestMessage(i, d.Torrent.PieceLength(i)))
}

func (d *dispatcher) sendInitialPieceRequests(c *conn) {
	for _, i := range d.Torrent.MissingPieces() {
		if !c.Bitfield.Has(i) {
			continue
		}
		if err := d.sendPieceRequest(c, i); err != nil {
			// Connection closed.
			break
		}
	}
}

// feed reads off of c and handles incoming messages. When c closes, the feed
// goroutine removes c from the dispatcher and exits.
func (d *dispatcher) feed(c *conn) {
	for msg := range c.Receiver() {
		if err := d.dispatch(c, msg); err != nil {
			d.log().Errorf("Error dispatching message: %s", err)
			// TODO Maybe close conn?
		}
	}
	d.logf(log.Fields{"peer": c.PeerID}).Debug("Dispatcher feeder exiting")
	d.conns.Delete(c.PeerID)
	d.touchLastConnRemoved()
}

func (d *dispatcher) dispatch(c *conn, msg *message) error {
	switch msg.Message.Type {
	case p2p.Message_ERROR:
		d.handleError(c, msg.Message.Error)
	case p2p.Message_ANNOUCE_PIECE:
		d.handleAnnouncePiece(c, msg.Message.AnnouncePiece)
	case p2p.Message_PIECE_REQUEST:
		d.handlePieceRequest(c, msg.Message.PieceRequest)
	case p2p.Message_PIECE_PAYLOAD:
		d.handlePiecePayload(c, msg.Message.PiecePayload, msg.Payload)
	case p2p.Message_CANCEL_PIECE:
		d.handleCancelPiece(c, msg.Message.CancelPiece)
	case p2p.Message_BITFIELD:
		d.handleBitfield(c, msg.Message.Bitfield)
	default:
		return fmt.Errorf("unknown message type: %d", msg.Message.Type)
	}
	return nil
}

func (d *dispatcher) handleError(c *conn, msg *p2p.ErrorMessage) {
	switch msg.Code {
	case p2p.ErrorMessage_PIECE_REQUEST_FAILED:
		d.log().Errorf("Piece request failed: %s", msg.Error)
		d.sendPieceRequest(c, int(msg.Index))
	}
}

func (d *dispatcher) handleAnnouncePiece(c *conn, msg *p2p.AnnouncePieceMessage) {
	if int(msg.Index) >= d.Torrent.NumPieces() {
		d.log().Errorf("Announce piece out of bounds: %d >= %d", msg.Index, d.Torrent.NumPieces())
		return
	}
	i := int(msg.Index)
	c.Bitfield.Set(i, true)
	if !d.Torrent.HasPiece(i) {
		d.sendPieceRequest(c, i)
	}
}

func (d *dispatcher) isFullPiece(i, offset, length int) bool {
	return offset == 0 && length == int(d.Torrent.PieceLength(i))
}

func (d *dispatcher) handlePieceRequest(c *conn, msg *p2p.PieceRequestMessage) {
	d.logf(log.Fields{"conn": c, "piece": msg.Index}).Debug("Received piece request")

	i := int(msg.Index)
	if !d.isFullPiece(i, int(msg.Offset), int(msg.Length)) {
		d.logf(log.Fields{"conn": c, "piece": i}).Error("Rejecting piece request: chunk not supported")
		c.Send(newErrorMessage(i, p2p.ErrorMessage_PIECE_REQUEST_FAILED, errChunkNotSupported))
		return
	}

	payload, err := d.Torrent.ReadPiece(i)
	if err != nil {
		d.logf(log.Fields{"conn": c, "piece": i}).Errorf("Error reading requested piece: %s", err)
		c.Send(newErrorMessage(i, p2p.ErrorMessage_PIECE_REQUEST_FAILED, err))
		return
	}
	if err := c.Send(newPiecePayloadMessage(i, payload)); err != nil {
		d.logf(log.Fields{"conn": c, "piece": i}).Errorf("Failed to send piece: %s", err)
		return
	}
	c.TouchLastPieceSent()
	d.logf(log.Fields{"conn": c, "piece": msg.Index}).Info("Sent piece")
}

func (d *dispatcher) handlePiecePayload(
	c *conn, msg *p2p.PiecePayloadMessage, payload []byte) {

	// TODO(codyg): re-request piece if write failed.

	i := int(msg.Index)
	if !d.isFullPiece(i, int(msg.Offset), int(msg.Length)) {
		d.logf(log.Fields{"conn": c, "piece": i}).Error("Rejecting piece payload: chunk not supported")
		return
	}
	if err := d.Torrent.WritePiece(payload, i); err != nil {
		if err != storage.ErrPieceComplete {
			d.logf(log.Fields{"conn": c, "piece": i}).Errorf("Error writing piece payload: %s", err)
		}
		return
	}
	d.logf(log.Fields{"conn": c, "piece": i}).Info("Received piece")
	d.networkEventProducer.Produce(
		networkevent.ReceivePieceEvent(d.Torrent.InfoHash(), d.localPeerID, c.PeerID, i))
	c.TouchLastGoodPieceReceived()
	if d.Torrent.Complete() {
		d.complete.Do(func() { go d.eventSender.Send(completedDispatcherEvent{d}) })
	}

	d.conns.Range(func(k, v interface{}) bool {
		if k.(torlib.PeerID) == c.PeerID {
			return true
		}
		cc := v.(*conn)

		d.logf(log.Fields{"conn": cc, "piece": i}).Info("Announcing piece")

		// Ignore error -- this just means the connection was closed. The feed goroutine
		// for cc will clean up.
		// TODO(codyg): We need to slim down the number of peers we announce a new
		// piece to.  We could just rely on announcing to the tracker instead of flooding
		// the network with tons of announce piece requests.
		cc.Send(newAnnouncePieceMessage(i))

		return true
	})
}

func (d *dispatcher) handleCancelPiece(c *conn, msg *p2p.CancelPieceMessage) {
	// No-op: cancelling not supported because all received messages are synchronized,
	// therefore if we receive a cancel it is already too late -- we've already read
	// the piece.
}

func (d *dispatcher) handleBitfield(c *conn, msg *p2p.BitfieldMessage) {
	log.WithFields(log.Fields{
		"peer": c.PeerID,
	}).Error("Unexpected bitfield message from established conn")
}

func (d *dispatcher) logf(f log.Fields) bark.Logger {
	f["torrent"] = d.Torrent
	return log.WithFields(f)
}

func (d *dispatcher) log() bark.Logger {
	return d.logf(log.Fields{})
}
