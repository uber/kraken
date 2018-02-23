package conn

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"code.uber.internal/infra/kraken/.gen/go/p2p"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"
)

// Maximum support protocol message size. Does not include piece payload.
const maxMessageSize = 32 * memsize.KB

// CloseHandler defines a function to be called when a Conn closes.
type CloseHandler func(*Conn)

// Conn manages peer communication over a connection for multiple torrents. Inbound
// messages are multiplexed based on the torrent they pertain to.
type Conn struct {
	peerID      core.PeerID
	infoHash    core.InfoHash
	createdAt   time.Time
	localPeerID core.PeerID

	closeHandler CloseHandler

	mu                    sync.Mutex // Protects the following fields:
	lastGoodPieceReceived time.Time
	lastPieceSent         time.Time

	// Controls egress piece bandwidth measured in bits.
	egressLimiter *rate.Limiter

	nc            net.Conn
	config        Config
	clk           clock.Clock
	stats         tally.Scope
	networkEvents networkevent.Producer

	// Marks whether the connection was opened by the remote peer, or the local peer.
	openedByRemote bool

	sender   chan *Message
	receiver chan *Message

	// The following fields orchestrate the closing of the connection:
	closeOnce sync.Once      // Ensures the close sequence is executed only once.
	done      chan struct{}  // Signals to readLoop / writeLoop to exit.
	wg        sync.WaitGroup // Waits for readLoop / writeLoop to exit.
}

func newConn(
	config Config,
	stats tally.Scope,
	clk clock.Clock,
	networkEvents networkevent.Producer,
	closeHandler CloseHandler,
	nc net.Conn,
	localPeerID core.PeerID,
	remotePeerID core.PeerID,
	info *storage.TorrentInfo,
	openedByRemote bool) (*Conn, error) {

	// Clear all deadlines set during handshake. Once a Conn is created, we
	// rely on our own idle Conn management via preemption events.
	if err := nc.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("set deadline: %s", err)
	}

	c := &Conn{
		peerID:       remotePeerID,
		localPeerID:  localPeerID,
		infoHash:     info.InfoHash(),
		createdAt:    clk.Now(),
		closeHandler: closeHandler,
		// A limit of 0 means no pieces will be allowed to send until bandwidth
		// is allocated with SetEgressBandwidthLimit.
		egressLimiter:  rate.NewLimiter(0, int(8*info.MaxPieceLength())),
		nc:             nc,
		config:         config,
		clk:            clk,
		stats:          stats,
		networkEvents:  networkEvents,
		openedByRemote: openedByRemote,
		sender:         make(chan *Message, config.SenderBufferSize),
		receiver:       make(chan *Message, config.ReceiverBufferSize),
		done:           make(chan struct{}),
	}

	c.start()

	return c, nil
}

// PeerID returns the remote peer id.
func (c *Conn) PeerID() core.PeerID {
	return c.peerID
}

// InfoHash returns the info hash for the torrent being transmitted over this
// connection.
func (c *Conn) InfoHash() core.InfoHash {
	return c.infoHash
}

// CreatedAt returns the time at which the Conn was created.
func (c *Conn) CreatedAt() time.Time {
	return c.createdAt
}

// SetEgressBandwidthLimit updates the egress bandwidth limit to bitsPerSec.
func (c *Conn) SetEgressBandwidthLimit(bitsPerSec uint64) {
	c.egressLimiter.SetLimitAt(c.clk.Now(), rate.Limit(float64(bitsPerSec)))
}

// GetEgressBandwidthLimit returns the current egress bandwidth limit.
func (c *Conn) GetEgressBandwidthLimit() uint64 {
	return uint64(c.egressLimiter.Limit())
}

// OpenedByRemote returns whether the Conn was opened by the local peer, or the remote peer.
func (c *Conn) OpenedByRemote() bool {
	return c.openedByRemote
}

func (c *Conn) String() string {
	return fmt.Sprintf("Conn(peer=%s, hash=%s, opened_by_remote=%t)",
		c.peerID, c.infoHash, c.openedByRemote)
}

// Active TODO(codyg)
func (c *Conn) Active() bool {
	return true
}

// Send writes the given message to the underlying connection.
func (c *Conn) Send(msg *Message) error {
	select {
	case <-c.done:
		return errors.New("conn closed")
	case c.sender <- msg:
		return nil
	default:
		// TODO(codyg): Consider a timeout here instead.

		switch msg.Message.Type {
		case p2p.Message_PIECE_REQUEST:
			c.networkEvents.Produce(
				networkevent.ConnSendDroppedPieceRequestEvent(
					c.infoHash, c.localPeerID, c.peerID, int(msg.Message.PieceRequest.Index)))
		case p2p.Message_PIECE_PAYLOAD:
			c.networkEvents.Produce(
				networkevent.ConnSendDroppedPiecePayloadEvent(
					c.infoHash, c.localPeerID, c.peerID, int(msg.Message.PiecePayload.Index)))
		}

		c.stats.Tagged(map[string]string{
			"dropped_message_type": msg.Message.Type.String(),
		}).Counter("dropped_messages").Inc(1)

		return errors.New("send buffer full")
	}
}

// Receiver returns a read-only channel for reading incoming messages off the connection.
func (c *Conn) Receiver() <-chan *Message {
	return c.receiver
}

// Close starts the shutdown sequence for the Conn.
func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		go func() {
			close(c.done)
			c.nc.Close()
			c.wg.Wait()
			c.closeHandler(c)
		}()
	})
}

func (c *Conn) start() {
	c.wg.Add(2)
	go c.readLoop()
	go c.writeLoop()
}

func (c *Conn) readPayload(length int32) ([]byte, error) {
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.nc, payload); err != nil {
		return nil, err
	}
	c.countBandwidth("ingress", int64(8*length))
	return payload, nil
}

func (c *Conn) readMessage() (*Message, error) {
	p2pMessage, err := readMessage(c.nc)
	if err != nil {
		return nil, fmt.Errorf("read message: %s", err)
	}
	var pr storage.PieceReader
	if p2pMessage.Type == p2p.Message_PIECE_PAYLOAD {
		// For payload messages, we must read the actual payload to the connection
		// after reading the message.
		payload, err := c.readPayload(p2pMessage.PiecePayload.Length)
		if err != nil {
			return nil, fmt.Errorf("read payload: %s", err)
		}
		// TODO(codyg): Consider making this reader read directly from the socket.
		pr = storage.NewPieceReaderBuffer(payload)
	}

	return &Message{p2pMessage, pr}, nil
}

// readLoop reads messages off of the underlying connection and sends them to the
// receiver channel.
func (c *Conn) readLoop() {
	defer func() {
		close(c.receiver)
		c.wg.Done()
		c.Close()
	}()

	for {
		select {
		case <-c.done:
			return
		default:
			msg, err := c.readMessage()
			if err != nil {
				c.log().Infof("Error reading message from socket, exiting read loop: %s", err)
				return
			}

			switch msg.Message.Type {
			case p2p.Message_PIECE_REQUEST:
				c.networkEvents.Produce(
					networkevent.ConnReaderGotPieceRequestEvent(
						c.infoHash, c.localPeerID, c.peerID, int(msg.Message.PieceRequest.Index)))
			case p2p.Message_PIECE_PAYLOAD:
				c.networkEvents.Produce(
					networkevent.ConnReaderGotPiecePayloadEvent(
						c.infoHash, c.localPeerID, c.peerID, int(msg.Message.PiecePayload.Index)))
			}

			c.receiver <- msg

			switch msg.Message.Type {
			case p2p.Message_PIECE_REQUEST:
				c.networkEvents.Produce(
					networkevent.ConnReaderSentPieceRequestEvent(
						c.infoHash, c.localPeerID, c.peerID, int(msg.Message.PieceRequest.Index)))
			case p2p.Message_PIECE_PAYLOAD:
				c.networkEvents.Produce(
					networkevent.ConnReaderSentPiecePayloadEvent(
						c.infoHash, c.localPeerID, c.peerID, int(msg.Message.PiecePayload.Index)))
			}

		}
	}
}

func (c *Conn) sendPiecePayload(pr storage.PieceReader) error {
	defer pr.Close()

	throttle := !c.config.DisableThrottling
	if throttle {
		nb := 8 * pr.Length()
		reserve := c.egressLimiter.ReserveN(c.clk.Now(), nb)
		if !reserve.OK() {
			// TODO(codyg): This is really bad. We need to alert if this happens.
			c.log("max_burst", c.egressLimiter.Burst(), "payload", nb).Errorf(
				"Cannot send piece, payload is larger than burst size")
			return errors.New("piece payload is larger than burst size")
		}
		// Throttle the connection egress if we've exceeded our bandwidth.
		c.clk.Sleep(reserve.DelayFrom(c.clk.Now()))
	}

	n, err := io.Copy(c.nc, pr)
	if err != nil {
		return fmt.Errorf("copy to socket: %s", err)
	}
	c.countBandwidth("egress", 8*n)
	return nil
}

func (c *Conn) sendMessage(msg *Message) error {
	if err := sendMessage(c.nc, msg.Message); err != nil {
		return fmt.Errorf("send message: %s", err)
	}
	if msg.Message.Type == p2p.Message_PIECE_PAYLOAD {
		// For payload messages, we must write the actual payload to the connection
		// after writing the message.
		if err := c.sendPiecePayload(msg.Payload); err != nil {
			return fmt.Errorf("send piece payload: %s", err)
		}
	}
	return nil
}

// writeLoop writes messages the underlying connection by pulling messages off of the sender
// channel.
func (c *Conn) writeLoop() {
	defer func() {
		c.wg.Done()
		c.Close()
	}()

	for {
		select {
		case <-c.done:
			return
		case msg := <-c.sender:

			switch msg.Message.Type {
			case p2p.Message_PIECE_REQUEST:
				c.networkEvents.Produce(
					networkevent.ConnSenderGotPieceRequestEvent(
						c.infoHash, c.localPeerID, c.peerID, int(msg.Message.PieceRequest.Index)))
			case p2p.Message_PIECE_PAYLOAD:
				c.networkEvents.Produce(
					networkevent.ConnSenderGotPiecePayloadEvent(
						c.infoHash, c.localPeerID, c.peerID, int(msg.Message.PiecePayload.Index)))
			}

			if err := c.sendMessage(msg); err != nil {
				c.log().Infof("Error writing message to socket, exiting write loop: %s", err)
				return
			}

			switch msg.Message.Type {
			case p2p.Message_PIECE_REQUEST:
				c.networkEvents.Produce(
					networkevent.ConnSenderSentPieceRequestEvent(
						c.infoHash, c.localPeerID, c.peerID, int(msg.Message.PieceRequest.Index)))
			case p2p.Message_PIECE_PAYLOAD:
				c.networkEvents.Produce(
					networkevent.ConnSenderSentPiecePayloadEvent(
						c.infoHash, c.localPeerID, c.peerID, int(msg.Message.PiecePayload.Index)))
			}

		}
	}
}

func (c *Conn) countBandwidth(direction string, n int64) {
	c.stats.Tagged(map[string]string{
		"piece_bandwidth_direction": direction,
	}).Counter("piece_bandwidth").Inc(n)
}

func (c *Conn) log(keysAndValues ...interface{}) *zap.SugaredLogger {
	keysAndValues = append(keysAndValues, "remote_peer", c.peerID, "hash", c.infoHash)
	return log.With(keysAndValues...)
}
