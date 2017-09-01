package scheduler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"github.com/andres-erbsen/clock"
	"github.com/golang/protobuf/proto"
	"github.com/uber-common/bark"

	"code.uber.internal/infra/kraken/.gen/go/p2p"
	"code.uber.internal/infra/kraken/client/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
)

const (
	_  = iota
	kb = 1 << 10 * iota
	mb
	gb
)

var (
	errHandshakeExpectedBitfield = errors.New(
		"handshaking new connection expected bitfield message")
	errHandshakeInfoHashMismatch = errors.New(
		"handshaking new connection received a mismatched info hash")
	errConnClosed            = errors.New("conn is closed")
	errEmptyPayload          = errors.New("payload is empty")
	errMessageExceedsMaxSize = errors.New("message exceeds max allowed size")
	errTorrentNotInConn      = errors.New("torrent not initialized for connection")
	errTorrentAlreadyInConn  = errors.New("torrent already initialized for connection")
)

// Maximum support protocol message size. Does not include piece payload.
const maxMessageSize = 32 * kb

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
	Config      Config
	LocalPeerID torlib.PeerID
	EventLoop   *eventLoop
	Clock       clock.Clock
}

// newConn resolves response handshake h into a new conn.
func (f *connFactory) newConn(
	nc net.Conn, h *handshake, openedByRemote bool) *conn {

	c := &conn{
		PeerID:         h.PeerID,
		InfoHash:       h.InfoHash,
		CreatedAt:      f.Clock.Now(),
		Bitfield:       newSyncBitfield(h.Bitfield),
		localPeerID:    f.LocalPeerID,
		nc:             nc,
		config:         f.Config,
		clock:          f.Clock,
		openedByRemote: openedByRemote,
		sender:         make(chan *message, f.Config.SenderBufferSize),
		receiver:       make(chan *message, f.Config.ReceiverBufferSize),
		eventLoop:      f.EventLoop,
		done:           make(chan struct{}),
	}

	c.start()

	return c
}

// SendAndReceiveHandshake initializes a new conn by sending localHandshake over
// nc and waiting for a handshake in response.
func (f *connFactory) SendAndReceiveHandshake(nc net.Conn, localHandshake *handshake) (*conn, error) {
	if err := sendMessage(nc, localHandshake.ToP2PMessage(), f.Clock, f.Config.WriteTimeout); err != nil {
		return nil, err
	}
	m, err := readMessage(nc)
	if err != nil {
		return nil, err
	}
	remoteHandshake, err := handshakeFromP2PMessage(m)
	if err != nil {
		return nil, err
	}
	if remoteHandshake.InfoHash != localHandshake.InfoHash {
		return nil, errHandshakeInfoHashMismatch
	}
	return f.newConn(nc, remoteHandshake, false), nil
}

// receiveHandshake reads a handshake from a new connection.
func receiveHandshake(nc net.Conn) (*handshake, error) {
	m, err := readMessage(nc)
	if err != nil {
		return nil, err
	}
	h, err := handshakeFromP2PMessage(m)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// ReciprocateHandshake initializes a new conn by sending localHandshake over nc
// assuming that remoteHandshake has already been received over nc.
func (f *connFactory) ReciprocateHandshake(
	nc net.Conn, remoteHandshake *handshake, localHandshake *handshake) (*conn, error) {

	if err := sendMessage(nc, localHandshake.ToP2PMessage(), f.Clock, f.Config.WriteTimeout); err != nil {
		return nil, err
	}
	return f.newConn(nc, remoteHandshake, true), nil
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

	// Tracks known pieces of the remote peer. Initialized to the bitfield sent
	// via handshake. Mainly used as a bookkeeping tool for dispatcher.
	Bitfield *syncBitfield

	localPeerID torlib.PeerID
	nc          net.Conn
	config      Config
	clock       clock.Clock

	// Marks whether the connection was opened by the remote peer, or the local peer.
	openedByRemote bool

	sender   chan *message
	receiver chan *message

	eventLoop *eventLoop

	// The following fields orchestrate the closing of the connection:
	once sync.Once      // Ensures the close sequence is executed only once.
	done chan struct{}  // Signals to readLoop / writeLoop to exit.
	wg   sync.WaitGroup // Waits for readLoop / writeLoop to exit.
}

func (c *conn) LastGoodPieceReceived() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastGoodPieceReceived
}

func (c *conn) TouchLastGoodPieceReceived() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastGoodPieceReceived = c.clock.Now()
}

func (c *conn) LastPieceSent() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastPieceSent
}

func (c *conn) TouchLastPieceSent() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastPieceSent = c.clock.Now()
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
			c.eventLoop.Send(closedConnEvent{c})
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
	return payload, nil
}

func readMessage(nc net.Conn) (*p2p.Message, error) {
	var msglen [4]byte
	if _, err := io.ReadFull(nc, msglen[:]); err != nil {
		return nil, err
	}
	dataLen := binary.BigEndian.Uint32(msglen[:])
	if dataLen > maxMessageSize {
		return nil, errMessageExceedsMaxSize
	}
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(nc, data); err != nil {
		return nil, err
	}
	p2pMessage := new(p2p.Message)
	if err := proto.Unmarshal(data, p2pMessage); err != nil {
		return nil, err
	}
	return p2pMessage, nil
}

func (c *conn) readMessage() (*message, error) {
	p2pMessage, err := readMessage(c.nc)
	if err != nil {
		return nil, err
	}
	var payload []byte
	if p2pMessage.Type == p2p.Message_PIECE_PAYLOAD {
		// For payload messages, we must read the actual payload to the connection
		// after reading the message.
		var err error
		payload, err = c.readPayload(p2pMessage.PiecePayload.Length)
		if err != nil {
			return nil, err
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

func (c *conn) sendPayload(payload []byte) error {
	if len(payload) == 0 {
		return errEmptyPayload
	}
	for len(payload) > 0 {
		n, err := c.nc.Write(payload)
		if err != nil {
			return err
		}
		payload = payload[n:]
	}
	return nil
}

func sendMessage(nc net.Conn, msg *p2p.Message, clk clock.Clock, timeout time.Duration) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	nc.SetWriteDeadline(clk.Now().Add(timeout))
	if err := binary.Write(nc, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	for len(data) > 0 {
		n, err := nc.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (c *conn) sendMessage(msg *message) error {
	if err := sendMessage(c.nc, msg.Message, c.clock, c.config.WriteTimeout); err != nil {
		return err
	}
	if msg.Message.Type == p2p.Message_PIECE_PAYLOAD {
		// For payload messages, we must write the actual payload to the connection
		// after writing the message.
		if err := c.sendPayload(msg.Payload); err != nil {
			return err
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
				c.log().Errorf("Error writing message to socket, closing connection: %s", err)
				break L
			}
		}
	}
	c.wg.Done()
	c.Close()
}

func (c *conn) logf(f log.Fields) bark.Logger {
	f["remote_peer"] = c.PeerID
	f["scheduler"] = c.localPeerID
	f["hash"] = c.InfoHash
	return log.WithFields(f)
}

func (c *conn) log() bark.Logger {
	return c.logf(log.Fields{})
}
