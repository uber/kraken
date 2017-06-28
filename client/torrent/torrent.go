package torrent

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"

	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/utils"

	"code.uber.internal/infra/kraken/client/torrent/bencode"
	"code.uber.internal/infra/kraken/client/torrent/meta"
	"code.uber.internal/infra/kraken/client/torrent/storage"
)

const (
	// None is a no event
	None AnnounceEvent = iota

	// Completed is a complete event
	Completed // The local peer just completed the torrent.

	// Started is a started event
	Started // The local peer has just resumed this torrent.

	// Stopped is stopped event
	Stopped // The local peer is leaving the swarm.
)

// Peer is an auxillary data structure to handle announcer response
// and mutate tracker peers into connections
type Peer struct {
	ID       [20]byte
	IP       net.IP
	Port     int
	Priority int
}

// peerKey in a handout
type peerKey struct {
	IPBytes string
	Port    int
}

// AnnounceEvent basic event classes: completed, in progress
type AnnounceEvent int32

// AnnounceRequest a structure represeneting a request to a tracker
type AnnounceRequest struct {
	InfoHash   [20]byte
	PeerID     [20]byte
	Downloaded int64
	Left       uint64
	Uploaded   int64
	// Apparently this is optional. None can be used for announces done at
	// regular intervals.
	Event     AnnounceEvent
	IPAddress int32
	Key       int32
	NumWant   int32 // How many peer addresses are desired. -1 for default.
	Port      uint16
} // 82 bytes

// AnnounceResponse a peer handout response from a tracker
type AnnounceResponse struct {
	Interval int32 // Minimum seconds the local peer should wait before next announce.
	Leechers int32
	Seeders  int32
	Peers    []Peer
}

type piece struct {
	completed bool
	hash      []byte
}

// Torrent maintains state of a particular torrent within a Client.
type Torrent struct {
	// client
	cl *Client

	// Torrent hash
	infoHash meta.Hash

	// Total length of the torrent in bytes. Stored because it's not O(1) to
	// get this from the info dict.
	length int64

	// Storage factory for torrent io
	storage storage.TorrentStorage

	// torrentIO to read/write torrent's data.
	torrentIO storage.Torrent

	// The info dict. nil if we don't have it (yet).
	info *meta.Info

	// max number of supported connections
	maxEstablishedConns int

	// close download channel
	closeOnce sync.Once

	// Signals when the Torrent has been closed.
	done chan struct{}

	// Mutex protects the following fields:
	sync.RWMutex
	pieces            []piece
	numPiecesComplete int
	// Pool of peers to connect to gets periodically updated from tracker
	peers map[peerKey]Peer
	// Active peer connections, running message stream loops.
	conns map[[20]byte]*Connection
}

func (e AnnounceEvent) String() string {
	// See BEP 3, "event".
	return []string{"empty", "completed", "started", "stopped"}[e]
}

// NewTorrent New Torrent's type contructor
func NewTorrent(client *Client, infoHash meta.Hash, infoBytes []byte,
	torrentStorage storage.TorrentStorage, maxConnPerTorrent int) (*Torrent, error) {

	h := meta.HashBytes(infoBytes)
	if h != infoHash {
		return nil, fmt.Errorf("info bytes have wrong hash: %v != %v", h, infoHash)
	}

	info := new(meta.Info)
	if err := bencode.Unmarshal(infoBytes, info); err != nil {
		return nil, fmt.Errorf("error unmarshalling info bytes: %s", err)
	}
	if err := info.Validate(); err != nil {
		return nil, fmt.Errorf("bad info: %s", err)
	}

	torrentIO, err := torrentStorage.OpenTorrent(info, infoHash)
	if err != nil {
		return nil, fmt.Errorf("error opening torrent storage: %s", err)
	}

	var length int64
	for _, f := range info.UpvertedFiles() {
		length += f.Length
	}

	pieces := make([]piece, info.NumPieces())
	sz := info.PieceHashSize()
	for i := range pieces {
		pieces[i].hash = make([]byte, sz)
		copy(pieces[i].hash, info.Pieces[i*sz:i*sz+sz])
	}

	t := &Torrent{
		cl:                  client,
		infoHash:            infoHash,
		length:              length,
		storage:             torrentStorage,
		torrentIO:           torrentIO,
		info:                info,
		maxEstablishedConns: maxConnPerTorrent,
		done:                make(chan struct{}),
		pieces:              pieces,
		peers:               make(map[peerKey]Peer),
		conns:               make(map[[20]byte]*Connection, 2*maxConnPerTorrent),
	}

	t.populateCompletedPieces()

	return t, nil

}

func (t *Torrent) markComplete(i int) {
	if t.pieces[i].completed {
		return
	}
	t.numPiecesComplete++
	t.pieces[i].completed = true
}

// InfoHash returns the Torrent's info has.
func (t *Torrent) InfoHash() meta.Hash {
	return t.infoHash
}

func (t *Torrent) String() string {
	// TODO(codyg): Move this to byte buffer.
	out := fmt.Sprintf("Torrent hash: %s", t.infoHash)
	out += fmt.Sprintf(" #pieces: %d", t.NumPieces())
	out += fmt.Sprintf(" #peers: %d", len(t.peers))

	return out
}

// Name returns the current working name for the Torrent.
func (t *Torrent) Name() string {
	return t.info.Name
}

// BytesLeft returns the number of bytes needed to complete the Torrent.
func (t *Torrent) BytesLeft() int64 {
	t.RLock()
	defer t.RUnlock()

	left := t.length
	for i := range t.pieces {
		if t.pieces[i].completed {
			left -= int64(t.pieceLength(i))
		}
	}
	return left
}

// bytesLeftAnnounce returns bytes left to give in tracker announces.
func (t *Torrent) bytesLeftAnnounce() uint64 {
	return uint64(t.BytesLeft())
}

// NumPieces overall number of pieces for the torrent
func (t *Torrent) NumPieces() int {
	return t.info.NumPieces()
}

// NumPiecesCompleted returns the number of completed pieces.
func (t *Torrent) NumPiecesCompleted() int {
	t.RLock()
	defer t.RUnlock()

	return t.numPiecesComplete
}

// IsComplete returns true if torrent is complete
func (t *Torrent) IsComplete() bool {
	t.RLock()
	defer t.RUnlock()

	return t.numPiecesComplete == t.NumPieces()
}

// PieceHash returns the hash for piece i.
func (t *Torrent) PieceHash(i int) []byte {
	t.RLock()
	defer t.RUnlock()

	return t.pieces[i].hash
}

// Close closes storage, connections
// and effectively torrent
func (t *Torrent) Close() error {
	t.Lock()
	defer t.Unlock()

	log.Info("torrent is closing")

	t.closeOnce.Do(func() {
		if t.storage != nil {
			log.Info("closing storage")
			t.storage.Close()
		}
		for _, conn := range t.conns {
			conn.Close()
			// Wait for connection to cleanly exit.
			conn.Wait()
		}
		close(t.done)
	})

	log.Info("torrent is closed")
	return nil
}

// Bitfield returns a bitmap of all pieces
func (t *Torrent) Bitfield() []bool {
	t.RLock()
	defer t.RUnlock()

	bf := make([]bool, t.NumPieces())
	for i := range t.pieces {
		bf[i] = t.pieces[i].completed
	}
	return bf
}

func (t *Torrent) pieceLength(i int) int64 {
	if i < 0 || i >= t.info.NumPieces() {
		log.Errorf("piece index is not valid %d out of %d", i, t.info.NumPieces())
		return 0
	}

	//the very last piece could be smaller
	if i == t.NumPieces()-1 {
		return t.length % t.info.PieceLength
	}
	return t.info.PieceLength
}

// PieceLength returns the length of piece i.
func (t *Torrent) PieceLength(i int) int64 {
	t.RLock()
	defer t.RUnlock()

	return t.pieceLength(i)
}

// VerifyPiece returns true if the given data is consistent with piece i's hash.
func (t *Torrent) VerifyPiece(i int, data []byte) bool {
	t.RLock()
	defer t.RUnlock()

	return t.verifyPiece(i, data)
}

func (t *Torrent) verifyPiece(i int, data []byte) bool {
	verifier := sha1.New()
	verifier.Write(data)
	sum := verifier.Sum(nil)
	return bytes.Compare(sum, t.pieces[i].hash) == 0
}

func (t *Torrent) populateCompletedPieces() {
	if t.NumPieces() == 0 {
		return
	}
	// at least piece 0 bytes
	data := make([]byte, t.info.PieceLength)
	for i := 0; i < t.NumPieces(); i++ {
		pl := t.pieceLength(i)
		n, err := t.readAt(data[:pl], int64(i)*t.info.PieceLength)
		if err != nil {
			log.Debugf(
				"Could not run verifier for piece %d for torrent %s: %v",
				i, t.infoHash, err)
			continue
		}
		if pl != int64(n) {
			log.Debugf(
				"Mismatched length for piece %d - requested %d, got %d",
				i, t.info.PieceLength, n)
			continue
		}

		// Last piece can be smaller than a piece length.
		if t.verifyPiece(i, data[:n]) {
			t.markComplete(i)
			log.Debugf("Verified piece %d for torrent %s", i, t.infoHash)
		} else {
			log.Debugf("Piece %d missing for torrnet %s", i, t.infoHash)
		}
	}
}

func (t *Torrent) readAt(b []byte, off int64) (int, error) {
	if t.torrentIO == nil {
		log.Error("torrentIO is not initialized")
		return 0, errors.New("torrentIO is not initialized")
	}
	return t.torrentIO.ReadAt(b, off)
}

// ReadPiece reads a chunk of piece i of the given length and at the given offset.
func (t *Torrent) ReadPiece(i, offset, length int) ([]byte, error) {
	absOffset := int64(i)*t.info.PieceLength + int64(offset)
	buf := make([]byte, length)
	if _, err := t.readAt(buf[:length], absOffset); err != nil {
		return nil, err
	}
	return buf, nil
}

func (t *Torrent) writeAt(b []byte, off int64) (n int, err error) {
	if t.torrentIO == nil {
		log.Error("torrentIO is not initialized")
		return 0, errors.New("torrentIO is not initialized")
	}
	return t.torrentIO.WriteAt(b, off)
}

func (t *Torrent) bytesCompleted() int64 {
	return t.info.TotalLength() - t.BytesLeft()
}

// DropConnection removes connection from the active pool
func (t *Torrent) DropConnection(c *Connection) {
	t.Lock()
	defer t.Unlock()

	delete(t.conns, c.PeerID)
}

// startScrapingTracker start a tracker server request/response loop
// should result in add peers to a torrent
// TODO: implement
func (t *Torrent) startScrapingTracker(url string) {

}

// announceRequest returns  AnnounceRequest object ready
// to be sent to a tracker server
func (t *Torrent) announceRequest() AnnounceRequest {
	ip, err := utils.AddrIP(t.cl.listenAddr)
	if err != nil {
		log.Errorf("cannot parse ip address: %s", err)
		panic(err)
	}
	ipUInt := binary.BigEndian.Uint32(ip)
	if len(ip) == 16 {
		ipUInt = binary.BigEndian.Uint32(ip[12:16])
	}

	return AnnounceRequest{
		Event:     None,
		NumWant:   -1,
		Port:      uint16(t.cl.incomingPeerPort()),
		PeerID:    t.cl.peerID,
		InfoHash:  t.infoHash,
		Left:      t.bytesLeftAnnounce(),
		IPAddress: int32(ipUInt),
	}
}

func (t *Torrent) popPeer() (Peer, bool) {
	if len(t.peers) == 0 {
		return Peer{}, false
	}
	var k peerKey
	var p Peer
	for k, p = range t.peers {
		break
	}
	delete(t.peers, k)
	return p, true
}

func (t *Torrent) openNewConns() {
	for {
		if !t.wantConns() {
			return
		}
		p, ok := t.popPeer()
		if !ok {
			return
		}
		// TODO(codyg): Eliminate this circular dependency.
		t.cl.initiateConn(p, t)
		log.Debugf("Initiate connection to %v", p)
	}
}

// OpenNewConns opens connections to available peers.
func (t *Torrent) OpenNewConns() {
	t.Lock()
	defer t.Unlock()

	t.openNewConns()
}

// AddPeers adds new peers to a tracker, initiate connection
// to them, preempts the bad ones
func (t *Torrent) AddPeers(peers []Peer) {
	t.Lock()
	defer t.Unlock()

	for _, p := range peers {
		k := peerKey{string(p.IP), p.Port}
		if _, ok := t.peers[k]; ok {
			continue
		}
		t.peers[k] = p
	}

	t.openNewConns()

	// TODO(codyg): Break apart the circular dependency between Client and Torrent
	// such that we know which connections were initialized.
	for i := len(t.pieces) - 1; i >= 0; i-- {
		for _, c := range t.conns {
			if !t.pieces[i].completed {
				go c.RequestPiece(i, t.pieceLength(i))
			}
		}
	}
}

// ErrConnectionAlreadyExists returns when a connection to a peer already exists.
type ErrConnectionAlreadyExists string

func (e ErrConnectionAlreadyExists) Error() string {
	return fmt.Sprintf("Connection already exists for peerID %v", e)
}

// ErrMaxConnections returns when no more connections can be added.
type ErrMaxConnections int

func (e ErrMaxConnections) Error() string {
	return fmt.Sprintf(
		"Cannot add connection, reached maximum capacity (%v) for established connections", e)
}

// AddConnection adds the given connection to the Torrent.
func (t *Torrent) AddConnection(c *Connection) error {
	t.Lock()
	defer t.Unlock()

	log.Debugf("AddConnection to %s", hex.EncodeToString(c.PeerID[:]))

	for peerID := range t.conns {
		if peerID == c.PeerID {
			return ErrConnectionAlreadyExists(peerID[:])
		}
	}
	if len(t.conns) >= t.maxEstablishedConns {
		return ErrMaxConnections(t.maxEstablishedConns)
	}

	c.t = t
	t.conns[c.PeerID] = c

	for i := len(t.pieces) - 1; i >= 0; i-- {
		if !t.pieces[i].completed {
			log.Debugf("Peer %s requesting piece %d", hex.EncodeToString(c.PeerID[:]), i)
			go c.RequestPiece(i, t.pieceLength(i))
		}
	}

	return nil
}

// wantConns is true when torrent needs data hence new connections
func (t *Torrent) wantConns() bool {
	return t.numPiecesComplete < t.NumPieces()
}

// PieceComplete marks piece i as complete and announces it to any peers.
func (t *Torrent) PieceComplete(i int) {
	t.Lock()
	defer t.Unlock()

	t.markComplete(i)

	for _, c := range t.conns {
		go c.AnnouncePiece(i)
		go c.CancelPieceRequest(i)
	}
}

// Wait blocks until the Torrent has finished closing.
func (t *Torrent) Wait() {
	<-t.done
}
