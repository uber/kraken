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
	"net"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/gen/go/proto/p2p"
	"github.com/uber/kraken/lib/torrent/networkevent"
	"github.com/uber/kraken/lib/torrent/storage"
	"github.com/uber/kraken/utils/bandwidth"

	"github.com/andres-erbsen/clock"
	"github.com/uber-go/tally"
	"github.com/willf/bitset"
	"go.uber.org/zap"
)

// RemoteBitfields represents the bitfields of an agent's peers for a given torrent.
type RemoteBitfields map[core.PeerID]*bitset.BitSet

func (rb RemoteBitfields) marshalBinary() (map[string][]byte, error) {
	rbBytes := make(map[string][]byte)
	for peerID, bitfield := range rb {
		b, err := bitfield.MarshalBinary()
		if err != nil {
			return nil, err
		}
		rbBytes[peerID.String()] = b
	}
	return rbBytes, nil
}

func (rb RemoteBitfields) unmarshalBinary(rbBytes map[string][]byte) error {
	for peerIDStr, bitfieldBytes := range rbBytes {
		peerID, err := core.NewPeerID(peerIDStr)
		if err != nil {
			return fmt.Errorf("peer id: %s", err)
		}
		bitfield := bitset.New(0)
		if err := bitfield.UnmarshalBinary(bitfieldBytes); err != nil {
			return err
		}
		rb[peerID] = bitfield
	}
	return nil
}

// handshake contains the same fields as a protobuf bitfield message, but with
// the fields converted into types used within the scheduler package. As such,
// in this package "handshake" and "bitfield message" are usually synonymous.
type handshake struct {
	peerID          core.PeerID
	digest          core.Digest
	infoHash        core.InfoHash
	bitfield        *bitset.BitSet
	remoteBitfields RemoteBitfields
	namespace       string
}

func (h *handshake) toP2PMessage() (*p2p.Message, error) {
	b, err := h.bitfield.MarshalBinary()
	if err != nil {
		return nil, err
	}
	rb, err := h.remoteBitfields.marshalBinary()
	if err != nil {
		return nil, err
	}
	return &p2p.Message{
		Type: p2p.Message_BITFIELD,
		Bitfield: &p2p.BitfieldMessage{
			PeerID:              h.peerID.String(),
			Name:                h.digest.Hex(),
			InfoHash:            h.infoHash.String(),
			BitfieldBytes:       b,
			RemoteBitfieldBytes: rb,
			Namespace:           h.namespace,
		},
	}, nil
}

func handshakeFromP2PMessage(m *p2p.Message) (*handshake, error) {
	if m.Type != p2p.Message_BITFIELD {
		return nil, fmt.Errorf("expected bitfield message, got %s", m.Type)
	}
	bitfieldMsg := m.GetBitfield()
	if bitfieldMsg == nil {
		return nil, fmt.Errorf("empty bit field")
	}
	peerID, err := core.NewPeerID(bitfieldMsg.PeerID)
	if err != nil {
		return nil, fmt.Errorf("peer id: %s", err)
	}
	ih, err := core.NewInfoHashFromHex(bitfieldMsg.InfoHash)
	if err != nil {
		return nil, fmt.Errorf("info hash: %s", err)
	}
	d, err := core.NewSHA256DigestFromHex(bitfieldMsg.Name)
	if err != nil {
		return nil, fmt.Errorf("name: %s", err)
	}
	bitfield := bitset.New(0)
	if err := bitfield.UnmarshalBinary(bitfieldMsg.BitfieldBytes); err != nil {
		return nil, err
	}
	remoteBitfields := make(RemoteBitfields)
	if err := remoteBitfields.unmarshalBinary(bitfieldMsg.RemoteBitfieldBytes); err != nil {
		return nil, err
	}

	return &handshake{
		peerID:          peerID,
		infoHash:        ih,
		bitfield:        bitfield,
		digest:          d,
		namespace:       bitfieldMsg.Namespace,
		remoteBitfields: remoteBitfields,
	}, nil
}

// PendingConn represents half-opened, pending connection initialized by a
// remote peer.
type PendingConn struct {
	handshake *handshake
	nc        net.Conn
}

// PeerID returns the remote peer id.
func (pc *PendingConn) PeerID() core.PeerID {
	return pc.handshake.peerID
}

// Digest returns the digest of the blob the remote peer wants to open.
func (pc *PendingConn) Digest() core.Digest {
	return pc.handshake.digest
}

// InfoHash returns the info hash of the torrent the remote peer wants to open.
func (pc *PendingConn) InfoHash() core.InfoHash {
	return pc.handshake.infoHash
}

// Bitfield returns the bitfield of the remote peer's torrent.
func (pc *PendingConn) Bitfield() *bitset.BitSet {
	return pc.handshake.bitfield
}

// RemoteBitfields returns the bitfield of the remote peer's torrent.
func (pc *PendingConn) RemoteBitfields() RemoteBitfields {
	return pc.handshake.remoteBitfields
}

// Namespace returns the namespace of the remote peer's torrent.
func (pc *PendingConn) Namespace() string {
	return pc.handshake.namespace
}

// Close closes the connection.
func (pc *PendingConn) Close() {
	pc.nc.Close()
}

// HandshakeResult wraps data returned from a successful handshake.
type HandshakeResult struct {
	Conn            *Conn
	Bitfield        *bitset.BitSet
	RemoteBitfields RemoteBitfields
}

// Handshaker defines the handshake protocol for establishing connections to
// other peers.
type Handshaker struct {
	config        Config
	stats         tally.Scope
	clk           clock.Clock
	bandwidth     *bandwidth.Limiter
	networkEvents networkevent.Producer
	peerID        core.PeerID
	events        Events
}

// NewHandshaker creates a new Handshaker.
func NewHandshaker(
	config Config,
	stats tally.Scope,
	clk clock.Clock,
	networkEvents networkevent.Producer,
	peerID core.PeerID,
	events Events,
	logger *zap.SugaredLogger) (*Handshaker, error) {

	config = config.applyDefaults()

	stats = stats.Tagged(map[string]string{
		"module": "conn",
	})

	bl, err := bandwidth.NewLimiter(config.Bandwidth, bandwidth.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("bandwidth: %s", err)
	}

	return &Handshaker{
		config:        config,
		stats:         stats,
		clk:           clk,
		bandwidth:     bl,
		networkEvents: networkEvents,
		peerID:        peerID,
		events:        events,
	}, nil
}

// Accept upgrades a raw network connection opened by a remote peer into a
// PendingConn.
func (h *Handshaker) Accept(nc net.Conn) (*PendingConn, error) {
	hs, err := h.readHandshake(nc)
	if err != nil {
		return nil, fmt.Errorf("read handshake: %s", err)
	}
	return &PendingConn{hs, nc}, nil
}

// Establish upgrades a PendingConn returned via Accept into a fully
// established Conn.
func (h *Handshaker) Establish(
	pc *PendingConn,
	info *storage.TorrentInfo,
	remoteBitfields RemoteBitfields) (*Conn, error) {

	// Namespace is one-directional: it is only supplied by the connection opener
	// and is not reciprocated by the connection acceptor.
	if err := h.sendHandshake(pc.nc, info, remoteBitfields, ""); err != nil {
		return nil, fmt.Errorf("send handshake: %s", err)
	}
	c, err := h.newConn(pc.nc, pc.handshake.peerID, info, true)
	if err != nil {
		return nil, fmt.Errorf("new conn: %s", err)
	}
	return c, nil
}

// Initialize returns a fully established Conn for the given torrent to the
// given peer / address. Also returns the bitfield of the remote peer and
// its connections for the torrent.
func (h *Handshaker) Initialize(
	peerID core.PeerID,
	addr string,
	info *storage.TorrentInfo,
	remoteBitfields RemoteBitfields,
	namespace string) (*HandshakeResult, error) {

	nc, err := net.DialTimeout("tcp", addr, h.config.HandshakeTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial: %s", err)
	}
	r, err := h.fullHandshake(nc, peerID, info, remoteBitfields, namespace)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return r, nil
}

func (h *Handshaker) sendHandshake(
	nc net.Conn,
	info *storage.TorrentInfo,
	remoteBitfields RemoteBitfields,
	namespace string) error {

	hs := &handshake{
		peerID:          h.peerID,
		digest:          info.Digest(),
		infoHash:        info.InfoHash(),
		bitfield:        info.Bitfield(),
		remoteBitfields: remoteBitfields,
		namespace:       namespace,
	}
	msg, err := hs.toP2PMessage()
	if err != nil {
		return err
	}
	return sendMessageWithTimeout(nc, msg, h.config.HandshakeTimeout)
}

func (h *Handshaker) readHandshake(nc net.Conn) (*handshake, error) {
	m, err := readMessageWithTimeout(nc, h.config.HandshakeTimeout)
	if err != nil {
		return nil, fmt.Errorf("read message: %s", err)
	}
	hs, err := handshakeFromP2PMessage(m)
	if err != nil {
		return nil, fmt.Errorf("handshake from p2p message: %s", err)
	}
	return hs, nil
}

func (h *Handshaker) fullHandshake(
	nc net.Conn,
	peerID core.PeerID,
	info *storage.TorrentInfo,
	remoteBitfields RemoteBitfields,
	namespace string) (*HandshakeResult, error) {

	if err := h.sendHandshake(nc, info, remoteBitfields, namespace); err != nil {
		return nil, fmt.Errorf("send handshake: %s", err)
	}
	hs, err := h.readHandshake(nc)
	if err != nil {
		return nil, fmt.Errorf("read handshake: %s", err)
	}
	if hs.peerID != peerID {
		return nil, errors.New("unexpected peer id")
	}
	c, err := h.newConn(nc, peerID, info, false)
	if err != nil {
		return nil, fmt.Errorf("new conn: %s", err)
	}
	return &HandshakeResult{c, hs.bitfield, hs.remoteBitfields}, nil
}

func (h *Handshaker) newConn(
	nc net.Conn,
	peerID core.PeerID,
	info *storage.TorrentInfo,
	openedByRemote bool) (*Conn, error) {

	return newConn(
		h.config,
		h.stats,
		h.clk,
		h.networkEvents,
		h.bandwidth,
		h.events,
		nc,
		h.peerID,
		peerID,
		info,
		openedByRemote,
		zap.NewNop().Sugar())
}
