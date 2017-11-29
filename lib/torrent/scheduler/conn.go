package scheduler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/golang/protobuf/proto"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"code.uber.internal/infra/kraken/.gen/go/p2p"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/memsize"
)

var (
	errHandshakeExpectedBitfield = errors.New(
		"handshaking new connection expected bitfield message")
	errConnClosed            = errors.New("conn is closed")
	errEmptyPayload          = errors.New("payload is empty")
	errMessageExceedsMaxSize = errors.New("message exceeds max allowed size")
	errTorrentNotInConn      = errors.New("torrent not initialized for connection")
	errTorrentAlreadyInConn  = errors.New("torrent already initialized for connection")
)

// Maximum support protocol message size. Does not include piece payload.
const maxMessageSize = 32 * memsize.KB

// handshake contains the same fields as a protobuf bitfield message, but with
// the fields converted into types used within the scheduler package. As such,
// in this package "handshake" and "bitfield message" are usually synonymous.
type handshake struct {
	PeerID   torlib.PeerID
	Name     string
	InfoHash torlib.InfoHash
	Bitfield storage.Bitfield
}

func (h *handshake) String() string {
	return fmt.Sprintf("handshake(peer=%s, hash=%s, name=%s, bitfield=%s)",
		h.PeerID, h.InfoHash, h.Name, h.Bitfield)
}

func (h *handshake) ToP2PMessage() *p2p.Message {
	return &p2p.Message{
		Type: p2p.Message_BITFIELD,
		Bitfield: &p2p.BitfieldMessage{
			PeerID:   h.PeerID.String(),
			Name:     h.Name,
			InfoHash: h.InfoHash.String(),
			Bitfield: h.Bitfield,
		},
	}
}

func handshakeFromP2PMessage(m *p2p.Message) (*handshake, error) {
	if m.Type != p2p.Message_BITFIELD {
		return nil, errHandshakeExpectedBitfield
	}
	peerID, err := torlib.NewPeerID(m.Bitfield.PeerID)
	if err != nil {
		return nil, err
	}

	ih, err := torlib.NewInfoHashFromHex(m.Bitfield.InfoHash)
	if err != nil {
		return nil, err
	}
	return &handshake{
		PeerID:   peerID,
		InfoHash: ih,
		Bitfield: m.Bitfield.Bitfield,
		Name:     m.Bitfield.Name,
	}, nil
}

type connFactory struct {
	Config      ConnConfig
	LocalPeerID torlib.PeerID
	EventSender eventSender
	Clock       clock.Clock
	Stats       tally.Scope
}

// newConn resolves response handshake h into a new conn.
func (f *connFactory) newConn(
	nc net.Conn,
	t storage.Torrent,
	remotePeerID torlib.PeerID,
	openedByRemote bool) (*conn, error) {

	// Clear all deadlines set during handshake. Once a conn is created, we
	// rely on our own idle conn management via preemption events.
	if err := nc.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("set deadline: %s", err)
	}

	c := &conn{
		PeerID:    remotePeerID,
		InfoHash:  t.InfoHash(),
		CreatedAt: f.Clock.Now(),
		// A limit of 0 means no pieces will be allowed to send until bandwidth
		// is allocated with SetEgressBandwidthLimit.
		egressLimiter:  rate.NewLimiter(0, int(t.MaxPieceLength())),
		localPeerID:    f.LocalPeerID,
		nc:             nc,
		config:         f.Config,
		clock:          f.Clock,
		stats:          f.Stats,
		openedByRemote: openedByRemote,
		sender:         make(chan *message, f.Config.SenderBufferSize),
		receiver:       make(chan *message, f.Config.ReceiverBufferSize),
		eventSender:    f.EventSender,
		done:           make(chan struct{}),
	}

	c.start()

	return c, nil
}

// SendAndReceiveHandshake initializes a new conn for Torrent t by sending a
// handshake over nc and waiting for a handshake in response.
func (f *connFactory) SendAndReceiveHandshake(nc net.Conn, t storage.Torrent) (*conn, storage.Bitfield, error) {
	localHandshake := &handshake{
		PeerID:   f.LocalPeerID,
		Name:     t.Name(),
		InfoHash: t.InfoHash(),
		Bitfield: t.Bitfield(),
	}
	if err := sendMessageWithTimeout(nc, localHandshake.ToP2PMessage(), f.Config.HandshakeTimeout); err != nil {
		return nil, nil, fmt.Errorf("failed to send handshake: %s", err)
	}
	m, err := readMessageWithTimeout(nc, f.Config.HandshakeTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to receive handshake: %s", err)
	}
	remoteHandshake, err := handshakeFromP2PMessage(m)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid handshake: %s", err)
	}
	if remoteHandshake.InfoHash != localHandshake.InfoHash {
		return nil, nil, errors.New("received handshake with incorrect info hash")
	}
	c, err := f.newConn(nc, t, remoteHandshake.PeerID, false)
	if err != nil {
		return nil, nil, fmt.Errorf("new conn: %s", err)
	}
	return c, remoteHandshake.Bitfield, nil
}

// receiveHandshake reads a handshake from a new connection.
func receiveHandshake(nc net.Conn, timeout time.Duration) (*handshake, error) {
	m, err := readMessageWithTimeout(nc, timeout)
	if err != nil {
		return nil, err
	}
	h, err := handshakeFromP2PMessage(m)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// ReciprocateHandshake initializes a new conn for Torrent t by sending a
// handshake over nc assuming that remoteHandshake has already been received
// over nc.
func (f *connFactory) ReciprocateHandshake(
	nc net.Conn, t storage.Torrent, remoteHandshake *handshake) (*conn, storage.Bitfield, error) {

	localHandshake := &handshake{
		PeerID:   f.LocalPeerID,
		Name:     t.Name(),
		InfoHash: t.InfoHash(),
		Bitfield: t.Bitfield(),
	}
	if err := sendMessageWithTimeout(nc, localHandshake.ToP2PMessage(), f.Config.HandshakeTimeout); err != nil {
		return nil, nil, fmt.Errorf("send message: %s", err)
	}
	c, err := f.newConn(nc, t, remoteHandshake.PeerID, true)
	if err != nil {
		return nil, nil, fmt.Errorf("new conn: %s", err)
	}
	return c, remoteHandshake.Bitfield, nil
}

// conn manages peer communication over a connection for multiple torrents. Inbound
// messages are multiplexed based on the torrent they pertain to.
type conn struct {
	PeerID    torlib.PeerID
	InfoHash  torlib.InfoHash
	CreatedAt time.Time

	mu                    sync.Mutex // Protects the following fields:
	lastGoodPieceReceived time.Time
	lastPieceSent         time.Time

	// Controls egress piece bandwidth.
	egressLimiter *rate.Limiter

	localPeerID torlib.PeerID
	nc          net.Conn
	config      ConnConfig
	clock       clock.Clock
	stats       tally.Scope

	// Marks whether the connection was opened by the remote peer, or the local peer.
	openedByRemote bool

	sender   chan *message
	receiver chan *message

	eventSender eventSender

	// The following fields orchestrate the closing of the connection:
	once sync.Once      // Ensures the close sequence is executed only once.
	done chan struct{}  // Signals to readLoop / writeLoop to exit.
	wg   sync.WaitGroup // Waits for readLoop / writeLoop to exit.
}

func (c *conn) SetEgressBandwidthLimit(bytesPerSec uint64) {
	c.egressLimiter.SetLimitAt(c.clock.Now(), rate.Limit(float64(bytesPerSec)))
}

func (c *conn) GetEgressBandwidthLimit() uint64 {
	return uint64(c.egressLimiter.Limit())
}

// OpenedByRemote returns whether the conn was opened by the local peer, or the remote peer.
func (c *conn) OpenedByRemote() bool {
	return c.openedByRemote
}

func (c *conn) String() string {
	return fmt.Sprintf("conn(peer=%s, hash=%s, opened_by_remote=%t)",
		c.PeerID, c.InfoHash, c.openedByRemote)
}

func (c *conn) Active() bool {
	// TODO
	return true
}

// Send writes the given message to the underlying connection.
func (c *conn) Send(msg *message) error {
	select {
	case <-c.done:
		return errConnClosed
	case c.sender <- msg:
		return nil
	}
}

// Receiver returns a read-only channel for reading incoming messages off the connection.
func (c *conn) Receiver() <-chan *message {
	return c.receiver
}

// Close starts the shutdown sequence for the conn.
func (c *conn) Close() {
	c.once.Do(func() {
		go func() {
			close(c.done)
			c.nc.Close()
			c.wg.Wait()
			c.eventSender.Send(closedConnEvent{c})
		}()
	})
}

func (c *conn) start() {
	c.wg.Add(2)
	go c.readLoop()
	go c.writeLoop()
}

func (c *conn) readPayload(length int32) ([]byte, error) {
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.nc, payload); err != nil {
		return nil, err
	}
	c.stats.Counter("ingress_piece_bandwidth").Inc(int64(length))
	return payload, nil
}

func readMessage(nc net.Conn) (*p2p.Message, error) {
	var msglen [4]byte
	if _, err := io.ReadFull(nc, msglen[:]); err != nil {
		return nil, fmt.Errorf("read message length: %s", err)
	}
	dataLen := binary.BigEndian.Uint32(msglen[:])
	if uint64(dataLen) > maxMessageSize {
		return nil, errMessageExceedsMaxSize
	}
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(nc, data); err != nil {
		return nil, fmt.Errorf("read data: %s", err)
	}
	p2pMessage := new(p2p.Message)
	if err := proto.Unmarshal(data, p2pMessage); err != nil {
		return nil, fmt.Errorf("proto unmarshal: %s", err)
	}
	return p2pMessage, nil
}

func readMessageWithTimeout(nc net.Conn, timeout time.Duration) (*p2p.Message, error) {
	// NOTE: We do not use the clock interface here because the net package uses
	// the system clock when evaluating deadlines.
	if err := nc.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set read deadline: %s", err)
	}
	return readMessage(nc)
}

func (c *conn) readMessage() (*message, error) {
	p2pMessage, err := readMessage(c.nc)
	if err != nil {
		return nil, fmt.Errorf("read message: %s", err)
	}
	var payload []byte
	if p2pMessage.Type == p2p.Message_PIECE_PAYLOAD {
		// For payload messages, we must read the actual payload to the connection
		// after reading the message.
		var err error
		payload, err = c.readPayload(p2pMessage.PiecePayload.Length)
		if err != nil {
			return nil, fmt.Errorf("read payload: %s", err)
		}
	}
	return &message{p2pMessage, payload}, nil
}

// readLoop reads messages off of the underlying connection and sends them to the
// receiver channel.
func (c *conn) readLoop() {
L:
	for {
		select {
		case <-c.done:
			break L
		default:
			msg, err := c.readMessage()
			if err != nil {
				c.log().Errorf("Error reading message from socket, closing connection: %s", err)
				break L
			}
			c.receiver <- msg
		}
	}
	close(c.receiver)
	c.wg.Done()
	c.Close()
}

func (c *conn) sendPiecePayload(b []byte) error {
	numBytes := len(b)
	if numBytes == 0 {
		return errEmptyPayload
	}

	if !c.config.DisableThrottling {
		r := c.egressLimiter.ReserveN(c.clock.Now(), numBytes)
		if !r.OK() {
			// TODO(codyg): This is really bad. We need to alert if this happens.
			c.log(
				"max_burst", c.egressLimiter.Burst(), "payload", numBytes,
			).Errorf("Cannot send piece, payload is larger than burst size")
			return errors.New("piece payload is larger than burst size")
		}

		// Throttle the connection egress if we've exceeded our bandwidth.
		c.clock.Sleep(r.DelayFrom(c.clock.Now()))
	}

	for len(b) > 0 {
		n, err := c.nc.Write(b)
		if err != nil {
			return fmt.Errorf("write: %s", err)
		}
		b = b[n:]
	}
	c.stats.Counter("egress_piece_bandwidth").Inc(int64(numBytes))
	return nil
}

func sendMessage(nc net.Conn, msg *p2p.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("proto marshal: %s", err)
	}
	if err := binary.Write(nc, binary.BigEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("write data length: %s", err)
	}
	for len(data) > 0 {
		n, err := nc.Write(data)
		if err != nil {
			return fmt.Errorf("write data: %s", err)
		}
		data = data[n:]
	}
	return nil
}

func sendMessageWithTimeout(nc net.Conn, msg *p2p.Message, timeout time.Duration) error {
	// NOTE: We do not use the clock interface here because the net package uses
	// the system clock when evaluating deadlines.
	if err := nc.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set write deadline: %s", err)
	}
	return sendMessage(nc, msg)
}

func (c *conn) sendMessage(msg *message) error {
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
func (c *conn) writeLoop() {
L:
	for {
		select {
		case <-c.done:
			break L
		case msg := <-c.sender:
			if err := c.sendMessage(msg); err != nil {
				c.log().Infof("Error writing message to socket, closing connection: %s", err)
				break L
			}
		}
	}
	c.wg.Done()
	c.Close()
}

func (c *conn) log(keysAndValues ...interface{}) *zap.SugaredLogger {
	keysAndValues = append(keysAndValues, "remote_peer", c.PeerID, "hash", c.InfoHash)
	return log.With(keysAndValues...)
}
