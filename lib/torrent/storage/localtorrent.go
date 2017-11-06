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
	_dirty
	_complete
)

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

// LocalTorrent implements a Torrent on top of a FileStore. Allows concurrent
// writes on distinct pieces, and concurrent reads on all pieces. Behavior is
// undefined if multiple LocalTorrent instances are backed by the same file
// store and metainfo.
type LocalTorrent struct {
	metaInfo    *torlib.MetaInfo
	store       store.FileStore
	pieces      []piece
	numComplete *atomic.Int32
}

// NewLocalTorrent creates a new LocalTorrent.
func NewLocalTorrent(store store.FileStore, mi *torlib.MetaInfo) (*LocalTorrent, error) {

	// We ignore existing download / metainfo file errors to allow thread
	// interleaving: if two threads try to create the same torrent at the same
	// time, said files will be created exactly once and both threads will succeed.

	if err := store.CreateDownloadFile(mi.Name(), mi.Info.Length); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("create download file: %s", err)
	}

	// Save metainfo in store so we do not need to query tracker everytime
	miRaw, err := mi.Serialize()
	if err != nil {
		return nil, fmt.Errorf("serialize metainfo: %s", err)
	}
	if _, err := store.SetDownloadOrCacheFileMeta(mi.Name(), []byte(miRaw)); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("write metainfo: %s", err)
	}

	t := &LocalTorrent{
		store:       store,
		metaInfo:    mi,
		pieces:      make([]piece, mi.Info.NumPieces()),
		numComplete: atomic.NewInt32(0),
	}

	t.restorePieces()

	if t.Complete() {
		if err := t.store.MoveDownloadFileToCache(mi.Name()); err != nil && !os.IsExist(err) {
			return nil, fmt.Errorf("move file to cache: %s", err)
		}
	}

	return t, nil
}

// restorePieces populates any existing piece state from file store. Must be called
// before any other read/write operations on t.
func (t *LocalTorrent) restorePieces() {
	f, err := t.store.GetDownloadOrCacheFileReader(t.metaInfo.Info.Name)
	if err != nil {
		log.Debugf("Restore pieces get file reader error: %s", err)
		return
	}
	defer f.Close()

	buf := make([]byte, t.metaInfo.Info.PieceLength)
	for i := range t.pieces {
		p := buf[:t.PieceLength(i)]
		if _, err := f.ReadAt(p, t.getFileOffset(i)); err != nil {
			continue
		}
		if err := t.verifyPiece(i, p); err != nil {
			continue
		}
		t.markPieceComplete(i)
	}
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
	for i := range t.pieces {
		if t.pieces[i].complete() {
			bitfield[i] = true
		}
	}
	return bitfield
}

func (t *LocalTorrent) String() string {
	downloaded := int(float64(t.BytesDownloaded()) / float64(t.metaInfo.Info.Length) * 100)
	return fmt.Sprintf("torrent(hash=%s, downloaded=%d%%)", t.InfoHash().HexString(), downloaded)
}

func (t *LocalTorrent) writePiece(data []byte, offset int64) error {
	f, err := t.store.GetDownloadFileReadWriter(t.metaInfo.Info.Name)
	if err != nil {
		return fmt.Errorf("cannot get download writer: %s", err)
	}
	defer f.Close()
	if _, err := f.WriteAt(data, offset); err != nil {
		return fmt.Errorf("write error: %s", err)
	}
	return nil
}

func (t *LocalTorrent) getPiece(pi int) (*piece, error) {
	if pi >= len(t.pieces) {
		return nil, fmt.Errorf("invalid piece index %d: num pieces = %d", pi, len(t.pieces))
	}
	return &t.pieces[pi], nil
}

// markPieceComplete must only be called once per piece.
func (t *LocalTorrent) markPieceComplete(pi int) {
	t.pieces[pi].markComplete()
	t.numComplete.Inc()
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

	if err := t.writePiece(data, t.getFileOffset(pi)); err != nil {
		// Allow other threads to write this piece since we mysteriously failed.
		piece.markEmpty()
		return fmt.Errorf("write piece: %s", err)
	}

	// Each piece will be marked complete exactly once.
	t.markPieceComplete(pi)

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
	for i := range t.pieces {
		if !t.pieces[i].complete() {
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
