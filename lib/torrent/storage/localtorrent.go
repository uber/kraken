package storage

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"sync"

	"go.uber.org/atomic"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/torlib"
)

// LocalTorrent errors.
var (
	ErrWritePieceConflict = errors.New("piece is already being written to")
	ErrPieceComplete      = errors.New("piece is already complete")
)

type pieceStatus int

const (
	_empty pieceStatus = iota
	_complete
	_dirty
)

func (s pieceStatus) toBytes() []byte { return []byte{byte(s)} }

type piece struct {
	sync.RWMutex
	status pieceStatus
}

func (p *piece) complete() bool {
	p.RLock()
	defer p.RUnlock()
	return p.status == _complete
}

func (p *piece) dirty() bool {
	p.RLock()
	defer p.RUnlock()
	return p.status == _dirty
}

func (p *piece) tryMarkDirty() (dirty, complete bool) {
	p.Lock()
	defer p.Unlock()

	switch p.status {
	case _empty:
		p.status = _dirty
	case _dirty:
		dirty = true
	case _complete:
		complete = true
	default:
		log.Fatalf("Unknown piece status: %d", p.status)
	}
	return
}

func (p *piece) markEmpty() {
	p.Lock()
	defer p.Unlock()
	p.status = _empty
}

func (p *piece) markComplete() {
	p.Lock()
	defer p.Unlock()
	p.status = _complete
}

func makeEmptyPieceMetadata(n int) []byte {
	return make([]byte, n)
}

func makeCompletePieceMetadata(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(_complete)
	}
	return b
}

// restorePieces reads piece metadata from disk and restores the in-memory piece
// statuses. A naive solution would be to read the entire blob from disk and
// hash the pieces to determine completion status -- however, this is very
// expensive. Instead, LocalTorrent tracks completed pieces on disk via metadata
// as they are written.
func restorePieces(name string, fs store.FileStore, numPieces int) (pieces []*piece, numComplete int, err error) {
	raw, err := fs.States().Download().GetOrSetMetadata(
		name, store.NewPieceStatus(), makeEmptyPieceMetadata(numPieces))
	if fs.InCacheError(err) {
		// File is in cache state -- initialize completed pieces.
		pieces = make([]*piece, numPieces)
		for i := range pieces {
			pieces[i] = &piece{status: _complete}
		}
		return pieces, numPieces, nil
	} else if err != nil {
		return nil, 0, fmt.Errorf("get or set piece metadata: %s", err)
	}

	pieces = make([]*piece, numPieces)
	for i, b := range raw {
		status := pieceStatus(b)
		if status != _empty && status != _complete {
			log.Errorf("Unexpected status at %d in piece metadata %s: %d", i, name, status)
			status = _empty
		}
		if status == _complete {
			numComplete++
		}
		pieces[i] = &piece{status: status}
	}
	return pieces, numComplete, nil
}

// LocalTorrent implements a Torrent on top of a FileStore. Allows concurrent
// writes on distinct pieces, and concurrent reads on all pieces. Behavior is
// undefined if multiple LocalTorrent instances are backed by the same file
// store and metainfo.
type LocalTorrent struct {
	metaInfo    *torlib.MetaInfo
	store       store.FileStore
	pieces      []*piece
	numComplete *atomic.Int32
}

// NewLocalTorrent creates a new LocalTorrent.
func NewLocalTorrent(store store.FileStore, mi *torlib.MetaInfo) (*LocalTorrent, error) {
	pieces, numComplete, err := restorePieces(mi.Name(), store, mi.Info.NumPieces())
	if err != nil {
		return nil, fmt.Errorf("restore pieces: %s", err)
	}

	if numComplete == len(pieces) {
		if err := store.MoveDownloadFileToCache(mi.Name()); err != nil && !os.IsExist(err) {
			return nil, fmt.Errorf("move file to cache: %s", err)
		}
	}

	return &LocalTorrent{
		store:       store,
		metaInfo:    mi,
		pieces:      pieces,
		numComplete: atomic.NewInt32(int32(numComplete)),
	}, nil
}

// Name returns the name of the target file.
func (t *LocalTorrent) Name() string {
	return t.metaInfo.Info.Name
}

// InfoHash returns the torrent metainfo hash.
func (t *LocalTorrent) InfoHash() torlib.InfoHash {
	return t.metaInfo.InfoHash
}

// NumPieces returns the number of pieces in the torrent.
func (t *LocalTorrent) NumPieces() int {
	return len(t.pieces)
}

// Length returns the length of the target file.
func (t *LocalTorrent) Length() int64 {
	return t.metaInfo.Info.Length
}

// PieceLength returns the length of piece pi.
func (t *LocalTorrent) PieceLength(pi int) int64 {
	if pi == len(t.pieces)-1 {
		// Last piece.
		return t.Length() - t.metaInfo.Info.PieceLength*int64(pi)
	}
	return t.metaInfo.Info.PieceLength
}

// MaxPieceLength returns the longest piece length of the torrent.
func (t *LocalTorrent) MaxPieceLength() int64 {
	return t.PieceLength(0)
}

// Complete indicates whether the torrent is complete or not.
func (t *LocalTorrent) Complete() bool {
	return int(t.numComplete.Load()) == len(t.pieces)
}

// BytesDownloaded returns an estimate of the number of bytes downloaded in the
// torrent.
func (t *LocalTorrent) BytesDownloaded() int64 {
	return min(int64(t.numComplete.Load())*t.metaInfo.Info.PieceLength, t.metaInfo.Info.Length)
}

// Bitfield returns the bitfield of pieces where true denotes a complete piece
// and false denotes an incomplete piece.
func (t *LocalTorrent) Bitfield() Bitfield {
	bitfield := make(Bitfield, len(t.pieces))
	for i, p := range t.pieces {
		if p.complete() {
			bitfield[i] = true
		}
	}
	return bitfield
}

func (t *LocalTorrent) String() string {
	downloaded := int(float64(t.BytesDownloaded()) / float64(t.metaInfo.Info.Length) * 100)
	return fmt.Sprintf("torrent(hash=%s, downloaded=%d%%)", t.InfoHash().HexString(), downloaded)
}

func (t *LocalTorrent) getPiece(pi int) (*piece, error) {
	if pi >= len(t.pieces) {
		return nil, fmt.Errorf("invalid piece index %d: num pieces = %d", pi, len(t.pieces))
	}
	return t.pieces[pi], nil
}

// markPieceComplete must only be called once per piece.
func (t *LocalTorrent) markPieceComplete(pi int) error {
	updated, err := t.store.States().Download().SetMetadataAt(
		t.Name(), store.NewPieceStatus(), _complete.toBytes(), pi)
	if err != nil {
		return fmt.Errorf("write piece metadata: %s", err)
	}
	if !updated {
		// This could mean there's another thread with a LocalTorrent instance using
		// the same file as us.
		log.Errorf("Invariant violation: piece marked complete twice: piece %d in %s", pi, t.Name())
	}
	t.pieces[pi].markComplete()
	t.numComplete.Inc()
	return nil
}

// writePiece writes data to piece pi. If the write succeeds, marks the piece as completed.
func (t *LocalTorrent) writePiece(data []byte, pi int) error {
	offset := t.getFileOffset(pi)
	f, err := t.store.GetDownloadFileReadWriter(t.metaInfo.Info.Name)
	if err != nil {
		return fmt.Errorf("cannot get download writer: %s", err)
	}
	defer f.Close()
	if _, err := f.WriteAt(data, offset); err != nil {
		return fmt.Errorf("write error: %s", err)
	}
	if err := t.markPieceComplete(pi); err != nil {
		return fmt.Errorf("mark piece complete: %s", err)
	}
	return nil
}

// WritePiece writes data to piece pi.
func (t *LocalTorrent) WritePiece(data []byte, pi int) error {
	piece, err := t.getPiece(pi)
	if err != nil {
		return err
	}
	if int64(len(data)) != t.PieceLength(pi) {
		return fmt.Errorf("invalid piece data length: expected %d, got %d", t.PieceLength(pi), len(data))
	}

	// Exit quickly if the piece is not writable.
	if piece.complete() {
		return ErrPieceComplete
	}
	if piece.dirty() {
		return ErrWritePieceConflict
	}

	if err := t.verifyPiece(pi, data); err != nil {
		return fmt.Errorf("invalid piece: %s", err)
	}

	dirty, complete := piece.tryMarkDirty()
	if dirty {
		return ErrWritePieceConflict
	} else if complete {
		return ErrPieceComplete
	}

	// At this point, we've determined that the piece is not complete and ensured
	// we are the only thread which may write the piece. We do not block other
	// threads from checking if the piece is writable.

	if err := t.writePiece(data, pi); err != nil {
		// Allow other threads to write this piece since we mysteriously failed.
		piece.markEmpty()
		return fmt.Errorf("write piece: %s", err)
	}

	if t.Complete() {
		// Multiple threads may attempt to move the download file to cache, however
		// only one will succeed while the others will receive (and ignore) file exist
		// error.
		if err := t.store.MoveDownloadFileToCache(t.metaInfo.Info.Name); err != nil && !os.IsExist(err) {
			return fmt.Errorf("download completed but failed to move file to cache directory: %s", err)
		}
	}

	return nil
}

// ReadPiece returns the data for piece pi.
func (t *LocalTorrent) ReadPiece(pi int) ([]byte, error) {
	piece, err := t.getPiece(pi)
	if err != nil {
		return nil, err
	}
	if !piece.complete() {
		return nil, errors.New("piece not complete")
	}

	// It is ok if file is moved from download to cache.
	f, err := t.store.GetDownloadOrCacheFileReader(t.metaInfo.Info.Name)
	if err != nil {
		return nil, fmt.Errorf("cannot get download/cache reader: %s", err)
	}
	defer f.Close()

	data := make([]byte, t.PieceLength(pi))
	if _, err := f.ReadAt(data, t.getFileOffset(pi)); err != nil {
		return nil, fmt.Errorf("read piece: %s", err)
	}
	return data, nil
}

// HasPiece returns if piece pi is complete.
func (t *LocalTorrent) HasPiece(pi int) bool {
	piece, err := t.getPiece(pi)
	if err != nil {
		return false
	}
	return piece.complete()
}

// MissingPieces returns the indeces of all missing pieces.
func (t *LocalTorrent) MissingPieces() []int {
	var missing []int
	for i, p := range t.pieces {
		if !p.complete() {
			missing = append(missing, i)
		}
	}
	return missing
}

// verifyPiece ensures data for pi is valid.
func (t *LocalTorrent) verifyPiece(pi int, data []byte) error {
	expectedHash, err := t.metaInfo.Info.PieceHash(pi)
	if err != nil {
		return fmt.Errorf("lookup piece hash: %s", err)
	}

	h := sha1.New()
	h.Write(data)
	b := h.Sum(nil)

	if bytes.Compare(b, expectedHash) != 0 {
		return errors.New("unexpected piece hash")
	}
	return nil
}

// getFileOffset calculates the offset in the torrent file given piece index.
// Assumes pi is a valid piece index.
func (t *LocalTorrent) getFileOffset(pi int) int64 {
	return t.metaInfo.Info.PieceLength * int64(pi)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
