package scheduler

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"sync"

	"code.uber.internal/infra/kraken/client/torrent/bencode"
	"code.uber.internal/infra/kraken/client/torrent/storage"
	"code.uber.internal/infra/kraken/torlib"
)

var (
	errInvalidInfoByteHash   = errors.New("info bytes has wrong hash")
	errInvalidPieceHash      = errors.New("piece has wrong hash")
	errPieceNotComplete      = errors.New("piece not complete")
	errPieceAlreadyCompleted = errors.New("piece already completed")
)

type piece struct {
	hash []byte
}

// Manages torrent state. This includes synchronizing reading and writing
// pieces to the torrent, and providing access to torrent torlibdata.
type torrent struct {
	InfoHash torlib.InfoHash
	info     *torlib.Info
	length   int64

	sync.RWMutex    // Synchronizes access to the following fields:
	store           storage.Torrent
	pieces          []piece
	completedPieces map[int]bool
	missingPieces   map[int]bool
}

func newTorrent(
	infoHash torlib.InfoHash,
	infoBytes []byte,
	store storage.Torrent) (*torrent, error) {

	h := torlib.NewInfoHashFromBytes(infoBytes)
	if h != infoHash {
		return nil, errInvalidInfoByteHash
	}

	info := new(torlib.Info)
	if err := bencode.Unmarshal(infoBytes, info); err != nil {
		return nil, err
	}
	if err := info.Validate(); err != nil {
		return nil, err
	}

	n := info.NumPieces()
	pieces := make([]piece, n)
	missingPieces := make(map[int]bool, n)
	completedPieces := make(map[int]bool, n)
	size := info.PieceHashSize()
	for i := 0; i < n; i++ {
		pieces[i].hash = make([]byte, size)
		copy(pieces[i].hash, info.Pieces[i*size:i*size+size])
		missingPieces[i] = true
	}

	t := &torrent{
		InfoHash:        infoHash,
		info:            info,
		length:          info.Length,
		store:           store,
		pieces:          pieces,
		completedPieces: completedPieces,
		missingPieces:   missingPieces,
	}
	t.populateCompletedPieces()

	return t, nil
}

func (t *torrent) Length() int64 {
	return t.length
}

func (t *torrent) Downloaded() int64 {
	t.RLock()
	defer t.RUnlock()

	var n int64
	for i := range t.completedPieces {
		n += t.pieceLength(i)
	}
	return n
}

func (t *torrent) Bitfield() []bool {
	t.RLock()
	defer t.RUnlock()

	bitfield := make([]bool, len(t.pieces))
	for i := range bitfield {
		bitfield[i] = t.completedPieces[i]
	}
	return bitfield
}

func (t *torrent) Complete() bool {
	t.RLock()
	defer t.RUnlock()

	return len(t.completedPieces) == t.info.NumPieces()
}

func (t *torrent) HasPiece(i int) bool {
	t.RLock()
	defer t.RUnlock()

	return t.completedPieces[i]
}

func (t *torrent) MissingPieces() []int {
	t.RLock()
	defer t.RUnlock()

	s := make([]int, 0, len(t.missingPieces))
	for i := range t.missingPieces {
		s = append(s, i)
	}
	return s
}

func (t *torrent) PieceLength(i int) int64 {
	return t.pieceLength(i)
}

func (t *torrent) NumPieces() int {
	return t.info.NumPieces()
}

func (t *torrent) ReadPiece(i int) ([]byte, error) {
	t.RLock()
	defer t.RUnlock()

	if !t.completedPieces[i] {
		return nil, errPieceNotComplete
	}
	buf := make([]byte, t.pieceLength(i))
	if _, err := t.store.ReadAt(buf, int64(i)*t.info.PieceLength); err != nil {
		return nil, err
	}
	return buf, nil
}

// WritePiece writes piece i with the given data. Returns true if torrent is completed
// after writing piece i.
func (t *torrent) WritePiece(i int, data []byte) (bool, error) {
	t.Lock()
	defer t.Unlock()

	if t.completedPieces[i] {
		return false, errPieceAlreadyCompleted
	}
	if err := t.verifyPiece(i, data); err != nil {
		return false, err
	}
	if _, err := t.store.WriteAt(data, int64(i)*t.info.PieceLength); err != nil {
		return false, err
	}
	t.markComplete(i)
	completed := len(t.completedPieces) == t.info.NumPieces()
	return completed, nil
}

func (t *torrent) String() string {
	return fmt.Sprintf("torrent(hash=%s, bitfield=%s)", t.InfoHash, formatBitfield(t.Bitfield()))
}

func (t *torrent) markComplete(i int) {
	delete(t.missingPieces, i)
	t.completedPieces[i] = true
}

func (t *torrent) verifyPiece(i int, data []byte) error {
	h := sha1.New()
	h.Write(data)
	b := h.Sum(nil)
	if bytes.Compare(b, t.pieces[i].hash) != 0 {
		return errInvalidPieceHash
	}
	return nil
}

func (t *torrent) pieceLength(i int) int64 {
	if i == t.info.NumPieces()-1 {
		// The last piece could be smaller.
		n := t.length % t.info.PieceLength
		if n > 0 {
			return n
		}
	}
	return t.info.PieceLength
}

// Marks any already complete pieces within storage as such in the torrent.
func (t *torrent) populateCompletedPieces() {
	buf := make([]byte, t.info.PieceLength)
	for i := 0; i < t.info.NumPieces(); i++ {
		pl := t.pieceLength(i)
		n, err := t.store.ReadAt(buf[:pl], int64(i)*t.info.PieceLength)
		if err != nil {
			continue
		}
		if pl != int64(n) {
			continue
		}

		// Last piece can be smaller than a piece length.
		if err = t.verifyPiece(i, buf[:pl]); err != nil {
			continue
		}
		t.markComplete(i)
	}
}
