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
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/gen/go/proto/p2p"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/lib/torrent/storage/piecereader"
	"github.com/uber/kraken/utils/bandwidth"
	"github.com/uber/kraken/utils/memsize"
)

// Maximum support protocol message size. Does not include piece payload.
const maxMessageSize = 32 * memsize.KB

// Events defines Conn events.
type Events interface {
	ConnClosed(*Conn)
}

// Conn manages peer communication over a connection for multiple torrents. Inbound
// messages are multiplexed based on the torrent they pertain to.
type Conn struct {
	peerID      core.PeerID
	infoHash    core.InfoHash
	createdAt   time.Time
	localPeerID core.PeerID
	bandwidth   *bandwidth.Limiter

	events Events

	mu                    sync.Mutex // Protects the following fields:
	lastGoodPieceReceived time.Time
	lastPieceSent         time.Time

	nc            net.Conn
	config        Config
	clk           clock.Clock
	stats         tally.Scope
	networkEvents networkevent.Producer

	// Marks whether the connection was opened by the remote peer, or the local peer.
	openedByRemote bool

	startOnce sync.Once

	sender   chan *Message
	receiver chan *Message

	// The following fields orchestrate the closing of the connection:
	closed *atomic.Bool
	done   chan struct{}  // Signals to readLoop / writeLoop to exit.
	wg     sync.WaitGroup // Waits for readLoop / writeLoop to exit.

	logger *zap.SugaredLogger
}

func newConn(
	config Config,
	stats tally.Scope,
	clk clock.Clock,
	networkEvents networkevent.Producer,
	bandwidth *bandwidth.Limiter,
	events Events,
	nc net.Conn,
	localPeerID core.PeerID,
	remotePeerID core.PeerID,
	info *storage.TorrentInfo,
	openedByRemote bool,
	logger *zap.SugaredLogger) (*Conn, error) {

	// Clear all deadlines set during handshake. Once a Conn is created, we
	// rely on our own idle Conn management via preemption events.
	if err := nc.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("set deadline: %s", err)
	}

	c := &Conn{
		peerID:         remotePeerID,
		infoHash:       info.InfoHash(),
		createdAt:      clk.Now(),
		localPeerID:    localPeerID,
		bandwidth:      bandwidth,
		events:         events,
		nc:             nc,
		config:         config,
		clk:            clk,
		stats:          stats,
		networkEvents:  networkEvents,
		openedByRemote: openedByRemote,
		sender:         make(chan *Message, config.SenderBufferSize),
		receiver:       make(chan *Message, config.ReceiverBufferSize),
		closed:         atomic.NewBool(false),
		done:           make(chan struct{}),
		logger:         logger,
	}

	return c, nil
}

// Start starts message processing on c. Note, once c has been started, it may
// close itself if it encounters an error reading/writing to the underlying
// socket.
func (c *Conn) Start() {
	c.startOnce.Do(func() {
		c.wg.Add(2)
		go c.readLoop()
		go c.writeLoop()
	})
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

func (c *Conn) String() string {
	return fmt.Sprintf("Conn(peer=%s, hash=%s, opened_by_remote=%t)",
		c.peerID, c.infoHash, c.openedByRemote)
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
	if !c.closed.CAS(false, true) {
		return
	}
	go func() {
		close(c.done)
		c.nc.Close()
		c.wg.Wait()
		c.events.ConnClosed(c)
	}()
}

// IsClosed returns true if the c is closed.
func (c *Conn) IsClosed() bool {
	return c.closed.Load()
}

func (c *Conn) readPayload(length int32) ([]byte, error) {
	if err := c.bandwidth.ReserveIngress(int64(length)); err != nil {
		c.log().Errorf("Error reserving ingress bandwidth for piece payload: %s", err)
		return nil, fmt.Errorf("ingress bandwidth: %s", err)
	}
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
		pr = piecereader.NewBuffer(payload)
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
			c.receiver <- msg
		}
	}
}

func (c *Conn) sendPiecePayload(pr storage.PieceReader) error {
	defer pr.Close()

	if err := c.bandwidth.ReserveEgress(int64(pr.Length())); err != nil {
		// TODO(codyg): This is bad. Consider alerting here.
		c.log().Errorf("Error reserving egress bandwidth for piece payload: %s", err)
		return fmt.Errorf("egress bandwidth: %s", err)
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
			if err := c.sendMessage(msg); err != nil {
				c.log().Infof("Error writing message to socket, exiting write loop: %s", err)
				return
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
	return c.logger.With(keysAndValues...)
}
