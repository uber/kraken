package torrent

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"code.uber.internal/go-common.git/x/log"

	p2p "code.uber.internal/infra/kraken/.gen/go/torrent"
	"code.uber.internal/infra/kraken/client/torrent/meta"
)

const (
	// MaxMessageSize defines a maximum support protocol message size
	MaxMessageSize = 32768 // max p2p protobuf message size, this does not include piece payload size

	// MaxConnWriteTimeout a default socket write timeout
	MaxConnWriteTimeout = 20
)

// error codes for a P2P protocol
const (
	UnknownError   = 0
	UnknownTorrent = 1
)

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

// PieceBitfield is a bitvector data structure
type PieceBitfield map[int]bool

// Connection maintains the state of a connection with a peer.
type Connection struct {
	//Refrence to a parent torrent object
	t *Torrent

	// reference to a client, we normally don't want to
	// access client object from the connection layer
	// it is nessesary during a handshake only on a receiving side
	client *Client

	// low level network connection object
	conn net.Conn

	// signal it's done
	done chan bool

	// PendingRequests for all pending requests from the local to remote peer in
	// a scope of this connection
	PendingRequests map[int]struct{}

	// PendingPeerRequests are all pending requests from the remote peer
	PendingPeerRequests map[int]struct{}

	// remote peer id
	PeerID [20]byte

	// The pieces the remote peer has claimed to have.
	peerBitfield PieceBitfield

	// MaxPendingRequests is the  number of pending requests the peer allows
	MaxPendingRequests int

	// outgoingMessages are peding outgoing message queue
	outgoingMessages chan *p2p.P2PMessage

	//Priority set by a tracker based on network topology proximity
	Priority int

	// pieceVerifier verifies piece digest
	pieceVerifier hash.Hash

	//connection mutex to serialize access to the connection members
	sync.RWMutex

	// close outgoing message queue channel
	closeOnce sync.Once
}

// NewConnection is a type constructor and initializer for connection
func NewConnection(torrent *Torrent, client *Client, conn net.Conn, maxPendingRequests int, priority int) (*Connection, error) {
	c := &Connection{
		t:                  torrent,
		conn:               conn,
		client:             client,
		PendingRequests:    make(map[int]struct{}, maxPendingRequests),
		outgoingMessages:   make(chan *p2p.P2PMessage),
		MaxPendingRequests: maxPendingRequests,
		Priority:           priority,
		pieceVerifier:      sha1.New(),
	}

	c.done = make(chan bool, 2)
	if torrent != nil {
		c.peerBitfield = make(map[int]bool, torrent.numPieces())
	}
	return c, nil
}

// Close closes the connection, fires all triggers to send
// notification out
func (cn *Connection) Close() {
	log.Debug("connection is closing...")

	cn.closeOnce.Do(func() {

		// close network connection
		cn.conn.Close()

		close(cn.outgoingMessages)
	})

	log.Debug("connection closed")
}

// PeerHasPiece returns true if a piece is known to a remote peer
func (cn *Connection) PeerHasPiece(pi int) bool {
	cn.Lock()
	defer cn.Unlock()

	_, ok := cn.peerBitfield[pi]

	return ok
}

// IsPieceRequestPending returns true if a piece is known to a local peer
func (cn *Connection) IsPieceRequestPending(pi int) bool {
	cn.Lock()
	defer cn.Unlock()

	_, ok := cn.PendingRequests[pi]

	return ok
}

// CancelIfPending returns true if a piece is known to a local peer
func (cn *Connection) CancelIfPending(pi int) bool {
	cn.Lock()
	defer cn.Unlock()

	if _, ok := cn.PendingRequests[pi]; ok {
		delete(cn.PendingRequests, pi)
		return ok
	}
	return false
}

// DropRequest returns true if a piece is known to a local peer
func (cn *Connection) DropRequest(pi int) {
	cn.Lock()
	defer cn.Unlock()

	delete(cn.PendingRequests, pi)
}

// RequestPiece puts a PieceRequest into a pending requests queue
// and sends a requests for a piece to a remote peer
func (cn *Connection) RequestPiece(pi int, length int64) {

	log.Infof("request a piece for t: %s, pi: %d", cn.t.infoHash, pi)

	go func() {
		cn.Lock()
		defer cn.Unlock()

		if len(cn.PendingRequests) >= cn.MaxPendingRequests {
			err := fmt.Errorf("# piece requets for a connection exceeds # max pending requests: %d",
				cn.MaxPendingRequests)
			log.Error(err)
		}
		cn.PendingRequests[pi] = struct{}{}
	}()

	message := &p2p.P2PMessage{
		Type: p2p.P2PMessage_PIECE_REQUEST,
		PieceRequest: &p2p.P2PMessage_PieceRequestMessage{
			Index:  int32(pi),
			Offset: 0,
			Length: int32(length)}}

	cn.writeMessage(message)
}

// CancelPieceRequest sends a cancel message to a remote peer
func (cn *Connection) CancelPieceRequest(pi int) error {
	log.Infof("cancel a piece request, t: %s, pi: %d", cn.t.infoHash, pi)

	if !cn.IsPieceRequestPending(pi) {
		return nil
	}
	cn.Lock()
	defer cn.Unlock()

	delete(cn.PendingRequests, pi)
	// TODO: do we actually need to send something to a remote peer here?
	return nil
}

// cancelPeerPieceRequest just clears local peer's queue from a request for
// a piece
func (cn *Connection) cancelPeerRequest(pi int) {
	cn.Lock()
	defer cn.Unlock()

	log.Infof("cancel a peer piece request for t: %s, pi: %d", cn.t.infoHash, pi)

	delete(cn.PendingPeerRequests, pi)
}

// AnnouncePiece advertises a piece to remote peer
func (cn *Connection) AnnouncePiece(pi int) {
	log.Infof("announce piece for t: %s, pi: %d", cn.t.infoHash, pi)

	cn.writeMessage(&p2p.P2PMessage{
		Type:          p2p.P2PMessage_ANNOUCE_PIECE,
		AnnouncePiece: &p2p.P2PMessage_AnnouncePieceMessage{Index: int32(pi)},
	})
}

// Bitfield sends a bitvector to a remote peer
// indicating what local peer claims so far
func (cn *Connection) Bitfield(bitfield []bool) {
	log.Infof("got a bitfield t: %s, bitfield: %s", cn.t.infoHash, bitfield)

	cn.writeMessage(&p2p.P2PMessage{
		Type:     p2p.P2PMessage_BITFIELD,
		Bitfield: &p2p.P2PMessage_BitfieldMessage{Bitfield: bitfield},
	})
}

// setBitfield sets a bitfield for remote peer
func (cn *Connection) setBitfield(bitfield []bool) error {
	cn.Lock()
	defer cn.Unlock()

	for i, v := range bitfield {
		cn.peerBitfield[i] = v
	}
	return nil
}

// sendMessage marshalls and sends a message into a socket
// please note this is an internal function, for all the cases
// use writeMessage, handshaking is the only exception
func (cn *Connection) sendMessage(message *p2p.P2PMessage) error {

	data, err := proto.Marshal(message)
	if err != nil {
		log.Errorf("proto marshaling error: %s", err)
		return err
	}

	cn.conn.SetWriteDeadline(time.Now().Add(MaxConnWriteTimeout * time.Second)) //20 secs write timeout
	//writer := bufio.NewWriter(cn.conn)

	err = binary.Write(cn.conn, binary.BigEndian, uint32(len(data)))
	if err != nil {
		log.Error("could not write data frame length")
		return err
	}

	// very rare, there are anecdotal evidences it still happens
	for n := 0; len(data) != 0 && err == nil; {
		n, err = cn.conn.Write(data)
		if err != nil {
			break
		}
		data = data[n:]
	}
	if err != nil {
		log.Error("could not write data frame message")
		return err
	}

	log.Debugf("sending a message: %s", message.String())

	// for a payload we need to send the actual piece right
	// after payload message
	if message.Type == p2p.P2PMessage_PIECE_PAYLOAD {
		err = cn.sendPiecePayload(
			int(message.PiecePayload.Index),
			int(message.PiecePayload.Offset),
			int(message.PiecePayload.Length))
		if err != nil {
			log.Errorf("failed to send a piece payload for %d, err: %s", message.PiecePayload.Index, err)
		}
	}
	return err
}

// readMessage reads and parses framed(len+message) protobuf P2PMessage
// TODO: make sure we don't hang in a read call forever, something
// probably in a torrent needs to check connections and close
// the hanging ones
func (cn *Connection) readMessage() (*p2p.P2PMessage, error) {
	//message framing

	// read message length
	var msglen [4]byte
	_, err := io.ReadFull(cn.conn, msglen[:])

	if err != nil {
		log.Errorf("cannot read a data length of a p2p message for torrent: %s, error: %s",
			cn.t.infoHash, err)
		return nil, err
	}

	// read the whole message
	dataLen := binary.BigEndian.Uint32(msglen[:])
	data := make([]byte, dataLen)

	log.Debugf("got a %d bytes message, peerid: %s", dataLen, hex.EncodeToString(cn.PeerID[:]))

	if dataLen > MaxMessageSize {
		err = fmt.Errorf("incoming message exceeds maximum message size, shutdown the connection, msg size: %d",
			dataLen)
		return nil, err
	}

	_, err = io.ReadFull(cn.conn, data)

	if err != nil {
		log.Errorf("cannot read a p2p message for torrent: %s, error: %s", cn.t.infoHash, err)
		return nil, err
	}

	message := &p2p.P2PMessage{}

	err = proto.Unmarshal(data, message)
	if err != nil {
		log.Errorf("could not parse p2p message for torrent: %s, with error: %s", cn.t.infoHash, err)
		return nil, err
	}
	log.Debugf("unmarshalled message %s", message)
	return message, nil
}

// writeMessage reads and parses framed(len+message) protobuf P2PMessage
// this is not thread safe and supposed to be called under mutex
func (cn *Connection) writeMessage(message *p2p.P2PMessage) {
	cn.outgoingMessages <- message
}

func (cn *Connection) handleErrorMessage(info string, index int, error string, code int) error {
	log.Errorf("got an error message from connection: (%s, %d, %s, %d)", info, index, error, code)
	return nil
}

// onAnnouncedPiece handles remote peer's announcment
func (cn *Connection) handleAnnouncePiece(pi int) error {
	log.Infof("on announce piece from a peer for t: %s, pi: %d", cn.t.infoHash, pi)

	cn.Lock()
	defer cn.Unlock()

	if pi >= len(cn.peerBitfield) {
		return errors.New("Piece index exceeds peer bitmap's capacity")
	}
	cn.peerBitfield[pi] = true
	return nil
}

// onPieceRequest handles remote peer's request for a piece
func (cn *Connection) handlePieceRequest(pi int, pieceOffset int, length int) error {
	log.Infof("on piece request from a peer for t: %s, pi: %d", cn.t.infoHash, pi)

	cn.Lock()
	_, ok := cn.PendingPeerRequests[pi]
	if ok { // we got already a request pending, this is a noop
		log.Debugf("onPieceRequest: request is already pending for %d", pi)
	}
	cn.Unlock()

	cn.writeMessage(&p2p.P2PMessage{
		Type: p2p.P2PMessage_PIECE_PAYLOAD,
		PiecePayload: &p2p.P2PMessage_PiecePayloadMessage{
			Info:   cn.t.infoHash.String(),
			Index:  int32(pi),
			Offset: int32(pieceOffset),
			Length: int32(length)},
	})
	return nil
}

func (cn *Connection) handleCancelPeerPieceRequest(pi int) {
	log.Infof("on cancel piece request from a peer for t: %s, pi: %d", cn.t.infoHash, pi)

	cn.Lock()
	defer cn.Unlock()

	delete(cn.PendingPeerRequests, pi)
}

// onPeerBitfield(bitfield handles remote peer's bitvector
func (cn *Connection) handlePeerBitfield(bitfield []bool) error {
	log.Infof("on peer bitfield for t: %s, bitfield: %s", cn.t.infoHash, bitfield)

	cn.Lock()
	defer cn.Unlock()

	for i, v := range bitfield {
		cn.peerBitfield[i] = v
	}
	return nil
}

// onReceivePiecePayload handles receiving a piece payload from a remote peer
// pieceOffset is 0 and length is set to a piece length
// these parameters to support a potential extension to chunked blob transfers if we ever need it
func (cn *Connection) handleReceivePiecePayload(pi int, pieceOffset int, length int, verify bool) error {
	log.Infof("on piece payload for t: %s, pi: %d, verify: %t", cn.t.infoHash, pi, verify)

	//init hash digest
	cn.pieceVerifier.Reset()

	//reader := bufio.NewReader(cn.conn)
	off := int64(pi)*cn.t.info.PieceLength + int64(pieceOffset)
	data := make([]byte, length)

	_, err := io.ReadFull(cn.conn, data)

	if err != nil {
		log.Errorf("could not successfully read a payload for a piece (%d, %d, %d): %s",
			pi, pieceOffset, length, err)
		return err
	}

	_, err = cn.t.writeAt(data, off)
	if err != nil {
		log.Errorf("could not write a payload piece (%d, %d, %d): %s",
			pi, pieceOffset, length, err)
		return err
	}

	if verify {
		cn.pieceVerifier.Write(data)
		if bytes.Compare(cn.t.pieceHashes[pi], cn.pieceVerifier.Sum(nil)) != 0 {
			err = fmt.Errorf("could not verify a piece: (%d, %d, %d), data:%s",
				pi, pieceOffset, length, string(data))
			log.Error(err)
		} else {
			log.Infof("verify piece (%d, %d, %d): ok",
				pi, pieceOffset, length)
		}
	}

	// drop the request from pending
	cn.Lock()
	delete(cn.PendingRequests, pi)
	cn.Unlock()

	if err == nil {
		cn.t.onPieceComplete(pi, cn)
	} else {
		cn.t.onPieceFailed(pi, cn)
	}

	log.Infof("received a piece payload (%d, %d, %d)", pi, pieceOffset, length)

	return err
}

// sendPiecePayload sends a piece payload message followed by an actual piece payload
func (cn *Connection) sendPiecePayload(pi int, pieceOffset int, length int) error {
	//writer := bufio.NewWriter(cn.conn)
	off := int64(pi)*cn.t.info.PieceLength + int64(pieceOffset)
	data := make([]byte, length)

	// This should read a piece fully or fail
	_, err := cn.t.readAt(data[:length], off)
	if err != nil {
		log.Errorf("could not read a payload for piece (%d, %d, %d): %s",
			pi, pieceOffset, length, err)
		return err
	}

	for len(data) > 0 {
		n, err := cn.conn.Write(data)
		if err != nil {
			break
		}
		data = data[n:]
	}
	if err != nil {
		log.Errorf("could not write a piece to stream (%d, %d, %d): %s",
			pi, pieceOffset, length, err)
		return err
	}
	log.Infof("sent a piece payload (%d, %d, %d)", pi, pieceOffset, length)
	return nil
}

// Writer is a go-routine that processes message from an
// message queue effectively representing an outgoing message queue
func (cn *Connection) Writer() {

	for message := range cn.outgoingMessages {
		if err := cn.sendMessage(message); err != nil {
			log.Errorf("could not write a message: %s", err)
			cn.Close()
		}
	}
	log.Info("shutdown writer")
	cn.done <- true
}

// dispatch processes incoming messages and calling correspnding handles
func (cn *Connection) dispatch(message *p2p.P2PMessage) error {
	var err error

	switch message.Type {
	case p2p.P2PMessage_ERROR:
		err = cn.handleErrorMessage(message.Error.Info, int(message.Error.Index),
			message.Error.Error, int(message.Error.Code))

	case p2p.P2PMessage_ANNOUCE_PIECE:
		err = cn.handleAnnouncePiece(int(message.AnnouncePiece.Index))

	case p2p.P2PMessage_PIECE_REQUEST:
		err = cn.handlePieceRequest(int(message.PieceRequest.Index),
			int(message.PieceRequest.Offset), int(message.PieceRequest.Length))

	case p2p.P2PMessage_PIECE_PAYLOAD:
		cn.handleReceivePiecePayload(int(message.PiecePayload.Index),
			int(message.PiecePayload.Offset), int(message.PiecePayload.Length), true)

	case p2p.P2PMessage_CANCEL_PIECE:
		cn.handleCancelPeerPieceRequest(int(message.CancelPiece.Index))

	case p2p.P2PMessage_BITFIELD:
		err = cn.handlePeerBitfield(message.Bitfield.Bitfield)
	default:
		err = fmt.Errorf("received unknown message type: %#v", message.Type)
		log.Error(err)
	}

	return err
}

// Reader is a go-routine that processes messages from an
// input message queue paasing them down to a dispatcher
func (cn *Connection) Reader() {
	for {
		// blocks here if there are no messages in a socket
		message, err := cn.readMessage()
		if err != nil && err != io.EOF {
			log.Errorf("got an error at reading the message from socket for %s, error %s", cn.t.infoHash, err)
			break
		} else if err == io.EOF {
			log.Errorf("connection is closed or in a process of being closed %s, err: %s", cn.t.infoHash, err)
			break
		} else {
			cn.dispatch(message)
			if err != nil {
				log.Errorf("connection got error, shutting down, t: %s, error :%s,", cn.t.infoHash, err)
				break
			}
		}
	}

	log.Info("shutdown reader")
	cn.Close()
	cn.done <- true
}

// ih is nil if we expect the peer to declare the InfoHash, such as when the
// peer initiated the connection otherwise it's us who initiate the connection
// and send request for torrent's hash and peerID.
// IMPORTANT: if we receive a handshake request, torrent and bitfield are not set
// for the connection. They only get set properly after a succesful handshake
// granted we got a torrent registerd locally. It is not safe to access
// cn.t before handshake is done in this case
func (cn *Connection) handshake(ih *meta.Hash, peerID []byte, bitfield []bool) (*meta.Hash, error) {
	log.Debug("handshaking a connection")

	var message *p2p.P2PMessage
	var err error

	if ih == nil {
		// read a bitfield message
		message, err = cn.readMessage()
		if err != nil {
			log.Errorf("1: handshake error: cannot read a message from incoming connection: %s", err)
			return nil, err
		}

		if message.Type != p2p.P2PMessage_BITFIELD {
			log.Errorf("1: handshake error: message type not expected, type= %d", message.Type)
			return nil, err
		}

		// we got a message request for a torrent that does not
		// exist, tell the peer to get lost
		if cn.t == nil && !cn.client.HasTorrent(message.Bitfield.Info) {
			log.Debug("1: cannot find a handshake torrent, ignore... ")

			err = cn.sendMessage(&p2p.P2PMessage{
				Type: p2p.P2PMessage_ERROR,
				Error: &p2p.P2PMessage_ErrorMessage{
					Info:  message.Bitfield.Info,
					Error: "Unknown torrent hash",
					Code:  UnknownTorrent},
			})
			return nil, err
		}

		// bind connection to a torrent
		t := cn.client.GetTorrent(message.Bitfield.Info)

		// connection is dangling at this point, nothing supposed
		// to get access to it
		cn.t = t
		cn.peerBitfield = make(map[int]bool, t.numPieces())

		// send torrent's bitfield reply
		err = cn.sendMessage(&p2p.P2PMessage{
			Type: p2p.P2PMessage_BITFIELD,
			Bitfield: &p2p.P2PMessage_BitfieldMessage{
				Info:     t.infoHash.String(),
				PeerID:   hex.EncodeToString(peerID),
				Bitfield: t.bitfield()},
		})

		if err != nil {
			log.Errorf("1: handshake error: cannot send a message to incoming connection: %s", err)
			return nil, err
		}
	} else {
		err := cn.sendMessage(&p2p.P2PMessage{
			Type: p2p.P2PMessage_BITFIELD,
			Bitfield: &p2p.P2PMessage_BitfieldMessage{
				Info:     ih.String(),
				PeerID:   hex.EncodeToString(peerID),
				Bitfield: bitfield},
		})

		if err != nil {
			log.Errorf("2: handshake error: outgoing connection's error: %s", err)
			return nil, err
		}

		message, err = cn.readMessage()
		if err != nil {
			log.Errorf("2: handshake error: cannot read a message from outgoing connection: %s", err)
			return nil, err
		}

		if message.Type != p2p.P2PMessage_BITFIELD {
			log.Errorf("2: handshake error: message type not expected, type= %d", message.Type)
			return nil, err
		}
	}

	// set remote peer's id
	n, err := hex.Decode(cn.PeerID[:], []byte(message.Bitfield.PeerID))
	if err != nil || n != len(cn.PeerID) {
		log.Errorf("handshake error: cannot decode peerID from a reply, len: %d, error: %s ", n, err)
		return nil, err
	}

	// set remote peer's bitfield
	cn.setBitfield(message.Bitfield.Bitfield)

	ihash := meta.NewHashFromHex(message.Bitfield.Info)

	log.Debug("handshake is successful: trying to add connection to the pool")
	return &ihash, err
}
