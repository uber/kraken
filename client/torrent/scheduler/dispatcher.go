package scheduler

import (
	"errors"
	"fmt"
	"sync"

	"github.com/uber-common/bark"

	"golang.org/x/sync/syncmap"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/.gen/go/p2p"
)

var (
	errPeerAlreadyDispatched   = errors.New("peer is already dispatched for the torrent")
	errPieceOutOfBounds        = errors.New("piece index out of bounds")
	errChunkNotSupported       = errors.New("reading / writing chunk of piece not supported")
	errRepeatedBitfieldMessage = errors.New("received repeated bitfield message")
)

type dispatcherFactory struct {
	LocalPeerID PeerID

	EventLoop *eventLoop
}

// New creates a new dispatcher for the given torrent.
func (f *dispatcherFactory) New(t *torrent) *dispatcher {
	d := &dispatcher{
		Torrent:     t,
		localPeerID: f.LocalPeerID,
		eventLoop:   f.EventLoop,
	}
	if t.Complete() {
		d.completed()
	}
	return d
}

// dispatcher coordinates torrent state with sending / receiving messages between multiple
// peers. As such, dispatcher and torrent have a one-to-one relationship, while dispatcher
// and conn have a many-to-many relationship.
type dispatcher struct {
	Torrent     *torrent
	localPeerID PeerID

	conns syncmap.Map

	// Ensures we only emit a complete event once.
	once sync.Once

	eventLoop *eventLoop
}

// AddConn registers a new conn with the dispatcher.
func (d *dispatcher) AddConn(c *conn) error {
	if c.InfoHash != d.Torrent.InfoHash {
		return errors.New("conn initialized for wrong torrent")
	}
	if _, ok := d.conns.LoadOrStore(c.PeerID, c); ok {
		return errors.New("conn already exists")
	}
	go d.sendInitialPieceRequests(c)
	go d.feed(c)
	return nil
}

func (d *dispatcher) String() string {
	return fmt.Sprintf("dispatcher(%s)", d.Torrent)
}

func (d *dispatcher) newPieceRequestMessage(i int) *message {
	return &message{
		Message: &p2p.Message{
			Type: p2p.Message_PIECE_REQUEST,
			PieceRequest: &p2p.PieceRequestMessage{
				Index:  int32(i),
				Offset: 0,
				Length: int32(d.Torrent.PieceLength(i)),
			},
		},
	}
}

func (d *dispatcher) sendInitialPieceRequests(c *conn) {
	d.logf(log.Fields{
		"peer": c.PeerID, "bitfield": c.Bitfield,
	}).Debug("Sending initial piece requests")
	for _, i := range d.Torrent.MissingPieces() {
		if !c.Bitfield.Has(i) {
			continue
		}
		d.logf(log.Fields{"peer": c.PeerID, "piece": i}).Debug("Sending piece request")
		m := d.newPieceRequestMessage(i)
		if err := c.Send(m); err != nil {
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
		c.Send(d.newPieceRequestMessage(int(msg.Index)))
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
		c.Send(d.newPieceRequestMessage(i))
	}
}

func (d *dispatcher) isFullPiece(i, offset, length int) bool {
	return offset == 0 && length == int(d.Torrent.PieceLength(i))
}

func (d *dispatcher) sendErrPieceRequestFailed(c *conn, i int32, err error) {
	m := &message{
		Message: &p2p.Message{
			Type: p2p.Message_ERROR,
			Error: &p2p.ErrorMessage{
				Index: i,
				Code:  p2p.ErrorMessage_PIECE_REQUEST_FAILED,
				Error: err.Error(),
			},
		},
	}
	c.Send(m)
}

func (d *dispatcher) handlePieceRequest(c *conn, msg *p2p.PieceRequestMessage) {
	d.logf(log.Fields{"peer": c.PeerID, "piece": msg.Index}).Debug("Received piece request")

	if !d.isFullPiece(int(msg.Index), int(msg.Offset), int(msg.Length)) {
		d.sendErrPieceRequestFailed(c, msg.Index, errChunkNotSupported)
		return
	}
	payload, err := d.Torrent.ReadPiece(int(msg.Index))
	if err != nil {
		d.sendErrPieceRequestFailed(c, msg.Index, err)
		return
	}
	m := &message{
		Message: &p2p.Message{
			Type: p2p.Message_PIECE_PAYLOAD,
			PiecePayload: &p2p.PiecePayloadMessage{
				Index:  msg.Index,
				Offset: 0,
				Length: int32(len(payload)),
			},
		},
		Payload: payload,
	}
	c.Send(m)
}

func (d *dispatcher) handlePiecePayload(
	c *conn, msg *p2p.PiecePayloadMessage, payload []byte) {

	// TODO(codyg): re-request piece if write failed.

	if !d.isFullPiece(int(msg.Index), int(msg.Offset), int(msg.Length)) {
		d.logf(log.Fields{
			"peer": c.PeerID,
		}).Errorf("Error handling piece payload: %s", errChunkNotSupported)
		return
	}
	if err := d.Torrent.WritePiece(int(msg.Index), payload); err != nil {
		d.logf(log.Fields{
			"peer": c.PeerID, "piece": msg.Index,
		}).Errorf("Error writing piece payload: %s", err)
		return
	}
	d.logf(log.Fields{
		"peer": c.PeerID, "piece": msg.Index,
	}).Debug("Downloaded piece payload")

	d.conns.Range(func(k, v interface{}) bool {
		if k.(PeerID) == c.PeerID {
			return true
		}
		cc := v.(*conn)

		// TODO(codyg): We need to slim down the number of peers we announce a new
		// piece to.  We could just rely on announcing to the tracker instead of flooding
		// the network with tons of announce piece requests.
		m := &message{
			Message: &p2p.Message{
				Type: p2p.Message_ANNOUCE_PIECE,
				AnnouncePiece: &p2p.AnnouncePieceMessage{
					Index: msg.Index,
				},
			},
		}

		d.logf(log.Fields{
			"peer": cc.PeerID, "hash": d.Torrent.InfoHash,
		}).Debugf("Announcing piece %d", msg.Index)

		// Ignore error -- this just means the connection was closed. The feed goroutine
		// for cc will clean up.
		cc.Send(m)

		return true
	})
	if d.Torrent.Complete() {
		d.completed()
	}
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

func (d *dispatcher) completed() {
	d.once.Do(func() {
		go d.eventLoop.Send(completedDispatcherEvent{d})
	})
}

func (d *dispatcher) logf(f log.Fields) bark.Logger {
	f["torrent"] = d.Torrent
	f["scheduler"] = d.localPeerID
	return log.WithFields(f)
}

func (d *dispatcher) log() bark.Logger {
	return d.logf(log.Fields{})
}
