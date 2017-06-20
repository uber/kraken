package torrent

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math"
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

// peersKey in a handout
type peersKey struct {
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

// Torrent maintains state of a particular torrent within a Client.
type Torrent struct {
	// client
	cl *Client

	// Torrent hash
	infoHash meta.Hash

	//Piece hashes
	pieceHashes map[int][]byte

	// Total length of the torrent in bytes. Stored because it's not O(1) to
	// get this from the info dict.
	length int64

	// Storage factory for torrent io
	storage storage.TorrentStorage

	// torrentIO to read/write torrent's data.
	torrentIO storage.Torrent

	// The info dict. nil if we don't have it (yet).
	info *meta.Info

	// Active peer connections, running message stream loops.
	conns map[*Connection]struct{}

	// Pool of peers to connect to
	// gets periodically updated from tracker
	peers map[peersKey]Peer

	// The indexes of pieces we want with normal priority, that aren't
	// currently available.
	pendingPieces map[int]struct{}

	// A cache of completed piece indices.
	completedPieces map[int]struct{}

	//connection mutex to serialize access to the torrent
	sync.RWMutex

	// download or close torrent event
	downloadCh chan struct{}

	//update notifier
	updateCh chan struct{}

	// max number of supported connections
	maxEstablishedConns int

	// close download channel
	closeOnce sync.Once
}

func (e AnnounceEvent) String() string {
	// See BEP 3, "event".
	return []string{"empty", "completed", "started", "stopped"}[e]
}

// NewTorrent New Torrent's type contructor
func NewTorrent(client *Client, infoHash meta.Hash, infoBytes []byte,
	torrentStorage storage.TorrentStorage, maxConnPerTorrent int) (*Torrent, error) {

	t := &Torrent{
		cl:                  client,
		infoHash:            infoHash,
		peers:               make(map[peersKey]Peer),
		conns:               make(map[*Connection]struct{}, 2*maxConnPerTorrent),
		storage:             torrentStorage,
		maxEstablishedConns: maxConnPerTorrent,
		downloadCh:          make(chan struct{}),
		updateCh:            make(chan struct{}),
	}
	if infoBytes != nil {
		if err := t.setInfoBytes(infoBytes); err != nil {
			return t, err
		}
	}

	// start the updater
	go t.updateRequests()

	return t, nil
}

func validateInfo(info *meta.Info) error {
	if len(info.Pieces)%20 != 0 {
		return errors.New("pieces has invalid length")
	}
	if info.PieceLength == 0 {
		if info.TotalLength() != 0 {
			return errors.New("zero piece length")
		}
	} else {
		if int((info.TotalLength()+info.PieceLength-1)/info.PieceLength) != info.NumPieces() {
			return errors.New("piece count and file lengths are at odds")
		}
	}
	return nil
}

func (t *Torrent) String() string {
	out := fmt.Sprintf("Torrent hash: %s", t.infoHash)
	out += fmt.Sprintf(" #pieces: %d", t.numPieces())
	out += fmt.Sprintf(" #peers: %d", len(t.peers))
	out += fmt.Sprintf(" #completed: %d", len(t.completedPieces))
	out += fmt.Sprintf(" #pending: %d", len(t.pendingPieces))

	return out
}

func (t *Torrent) addPeer(p Peer) {
	cl := t.cl
	key := peersKey{string(p.IP), p.Port}
	if _, ok := t.peers[key]; ok {
		return
	}
	t.peers[key] = p
	cl.openNewConns(t)

}

func (t *Torrent) infoPieceHashes(info *meta.Info) {
	t.pieceHashes = make(map[int][]byte)
	verifier := sha1.New()

	sz := verifier.Size()
	for i := 0; i < len(info.Pieces)/sz; i++ {
		t.pieceHashes[i] = make([]byte, sz)
		copy(t.pieceHashes[i], info.Pieces[i*sz:i*sz+sz])
	}
}

// Called when metadata for a torrent becomes available.
func (t *Torrent) setInfoBytes(b []byte) error {
	t.Lock()
	defer t.Unlock()

	if t.haveInfo() {
		return nil
	}
	if meta.HashBytes(b) != t.infoHash {
		return errors.New("info bytes have wrong hash")
	}
	var info meta.Info
	err := bencode.Unmarshal(b, &info)
	if err != nil {
		return fmt.Errorf("error unmarshalling info bytes: %s", err)
	}
	err = validateInfo(&info)
	if err != nil {
		return fmt.Errorf("bad info: %s", err)
	}
	t.info = &info
	t.torrentIO, err = t.storage.OpenTorrent(t.info, t.infoHash)
	if err != nil {
		return fmt.Errorf("error opening torrent storage: %s", err)
	}
	t.length = 0
	for _, f := range t.info.UpvertedFiles() {
		t.length += f.Length
	}

	t.pendingPieces = make(map[int]struct{}, t.numPieces())
	t.completedPieces = make(map[int]struct{}, t.numPieces())

	// fill in piece hashes from torrent info
	t.infoPieceHashes(&info)

	// fill in completed pieces from storage, verify their hashes
	t.checkCompletePieces(true)

	// TODO: start torrent announcer request to get peers
	// otherwise this is non actionable
	return nil
}

// name current working name for the torrent.
func (t *Torrent) name() string {
	return t.info.Name
}

// haveInfo return true if torrent has meta info
func (t *Torrent) haveInfo() bool {
	return t.info != nil
}

// bytesLeft returns amount of bytes left
func (t *Torrent) bytesLeft() (left int64) {
	left = t.length
	for i := range t.completedPieces {
		left -= int64(t.pieceLength(i))
	}
	return left
}

// bytesLeftAnnounce returns bytes left to give in tracker announces.
func (t *Torrent) bytesLeftAnnounce() uint64 {
	if t.haveInfo() {
		return uint64(t.bytesLeft())
	}
	return math.MaxUint64
}

// numPieces: overall number of pieces for the torrent
func (t *Torrent) numPieces() int {
	return t.info.NumPieces()
}

// numPiecesCompleted returns #pieces completed
func (t *Torrent) numPiecesCompleted() (num int) {
	t.RLock()
	defer t.Unlock()

	return len(t.completedPieces)
}

// IsComplete returns true if torrent is complete
func (t *Torrent) IsComplete() bool {
	t.RLock()
	defer t.RUnlock()

	return len(t.completedPieces) == t.numPieces()
}

// Close closes storage, connections
// and effectively torrent
func (t *Torrent) Close() (err error) {
	log.Info("torrent is closing")

	t.closeOnce.Do(func() {
		if t.storage != nil {
			log.Info("closing storage")
			t.storage.Close()
		}

		for conn := range t.conns {
			conn.Close()
		}
		close(t.downloadCh)

	})

	log.Info("torrent is closed")
	return nil
}

// bitfield returns a bitmap of all pieces
func (t *Torrent) bitfield() (bf []bool) {
	bf = make([]bool, t.numPieces())
	for i := range t.completedPieces {
		bf[i] = true
	}
	return bf
}

// pieceLength retuens a piece length
func (t *Torrent) pieceLength(pi int) int64 {
	if pi < 0 || pi >= t.info.NumPieces() {
		log.Errorf("piece index is not valid %d out of %d", pi, t.info.NumPieces())
		return 0
	}

	//the very last piece could be smaller
	if pi == t.numPieces()-1 {
		return t.length % t.info.PieceLength
	}
	return t.info.PieceLength
}

// openNewConns a new connection
func (t *Torrent) openNewConns() {
	t.cl.openNewConns(t)
}

// verifyPiece verifies a piece
func (t *Torrent) verifyPiece(verifier hash.Hash, data []byte, pi int) bool {
	verifier.Reset()
	verifier.Write(data)

	hashSum := verifier.Sum(nil)
	if bytes.Compare(hashSum, t.pieceHashes[pi]) == 0 {
		t.completedPieces[pi] = struct{}{}
		return true
	}
	t.pendingPieces[pi] = struct{}{}
	return false
}

// checkCompletePieces verifies completed peices
func (t *Torrent) checkCompletePieces(verify bool) {
	if t.numPieces() == 0 {
		return
	}

	pieceVerifier := sha1.New()

	// at least piece 0 bytes
	data := make([]byte, t.info.PieceLength)
	for i := 0; i < t.numPieces(); i++ {
		pl := t.pieceLength(i)
		n, err := t.readAt(data[:pl], int64(i)*t.info.PieceLength)

		// last piece can be smaller than a piece length
		if err == nil && pl == int64(n) {
			if verify {
				good := t.verifyPiece(pieceVerifier, data[:n], i)
				log.Infof("verified piece %d: ,result: %t, torrent %s", i, good, t.infoHash)
			} else {
				t.pendingPieces[i] = struct{}{}
			}
		} else {
			log.Errorf("could not run verifier due to mismatched length or error at readAt, requested: %d, got: %d, torrent %s, error: %s",
				t.info.PieceLength, n, t.infoHash, err)
		}
	}
}

// readAt (ReaderAt interface) reads data from a default astorage
func (t *Torrent) readAt(b []byte, off int64) (n int, err error) {
	if t.torrentIO == nil {
		log.Error("torrentIO is not initialized")
		return 0, errors.New("torrentIO is not initialized")
	}
	return t.torrentIO.ReadAt(b, off)
}

// writeAt (WriterAt interface) writes data into default astorage
func (t *Torrent) writeAt(b []byte, off int64) (n int, err error) {
	if t.torrentIO == nil {
		log.Error("torrentIO is not initialized")
		return 0, errors.New("torrentIO is not initialized")
	}
	return t.torrentIO.WriteAt(b, off)
}

// needData returns true if torrent still needs data
func (t *Torrent) needData() bool {
	return len(t.completedPieces) < t.numPieces()
}

// bytesCompleted retursn # bytes downloaded so far
func (t *Torrent) bytesCompleted() int64 {
	return t.info.TotalLength() - t.bytesLeft()
}

// SetInfoBytes sets torrent blob's data
func (t *Torrent) SetInfoBytes(b []byte) (err error) {
	return t.setInfoBytes(b)
}

// dropConnection removes connection from the active pool
func (t *Torrent) dropConnection(c *Connection) {
	log.Info("drop a connection")
	if _, ok := t.conns[c]; ok {
		delete(t.conns, c)
	}
	//t.openNewConns()
}

// wantPeers is true if torrent still need data
func (t *Torrent) wantPeers() bool {
	return t.needData()
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

// AddPeers adds new peers to a tracker, initiate connection
// to them, preempts the bad ones
func (t *Torrent) AddPeers(peers []Peer) {
	for _, p := range peers {
		t.addPeer(p)
	}
	t.updateCh <- struct{}{}
}

// updateRequests iterates through all the pieces
// to determine which one needs to be requested and in
// what order
func (t *Torrent) updateRequests() {
	for {
		t.Lock()

		log.Infof("updates torrent requests: %s", t.infoHash)
		for i := t.numPieces() - 1; i >= 0; i-- {

			_, complete := t.completedPieces[i]
			_, pending := t.pendingPieces[i]

			for conn := range t.conns {

				// onComplete handler
				if complete && pending {
					delete(t.pendingPieces, i)

					conn.AnnouncePiece(i)
					conn.CancelPieceRequest(i)
				}

				// new or failed: need to make a request
				if !complete && !pending {
					conn.RequestPiece(i, t.pieceLength(i))
				}
			}
		}
		t.Unlock()

		<-t.updateCh
	}
}

// addConnection returns true if the connection is added successfully
func (t *Torrent) addConnection(c *Connection) bool {
	t.Lock()
	defer t.Unlock()

	for c0 := range t.conns {
		if c.PeerID == c0.PeerID {
			log.Errorf("already have a connection with the same peerID: %s", hex.EncodeToString(c.PeerID[:]))
			return false
		}
	}

	// TODO(igor): implement connection dropping logic
	// based on the idea of better/worse connections(priority, #errors)
	if len(t.conns) >= t.maxEstablishedConns {
		log.Errorf("Cannot ad a connection: reached maximum capacity for established connections: %d", t.maxEstablishedConns)
		return false
	}
	c.t = t
	t.conns[c] = struct{}{}

	log.Debug("connection added to a pool")
	return true
}

// wantConns is true when torrent needs data hence new connections
func (t *Torrent) wantConns() bool {
	if !t.needData() {
		return false
	}
	return true
}

// onPieceComplete is a handler for when piece is received and verified
func (t *Torrent) onPieceComplete(pi int, c *Connection) {
	log.Infof("onPieceComplete: %d", pi)

	t.Lock()
	t.completedPieces[pi] = struct{}{}
	t.Unlock()

	// updater will announce a piece among al the connections
	// and will remove it from pending
	t.updateCh <- struct{}{}
}

// onPiecePending is a handler for when piece is requested
// but not yet received
func (t *Torrent) onPiecePending(pi int, c *Connection) {
}

// onPieceFailed is a handler gets triggered when peice was
// not downloaded successfully
func (t *Torrent) onPieceFailed(pi int, c *Connection) {
	log.Infof("onPieceFailed: %d", pi)

	t.Lock()
	delete(t.pendingPieces, pi)
	t.Unlock()

	t.updateCh <- struct{}{}
}

// Wait for close event
func (t *Torrent) Wait() {
	<-t.downloadCh
}
