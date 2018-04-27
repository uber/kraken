package agentstorage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/lib/torrent/storage/piecereader"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/willf/bitset"
	"go.uber.org/atomic"
)

var errWritePieceConflict = errors.New("piece is already being written to")

type pieceStatus int

const (
	_empty pieceStatus = iota
	_complete
	_dirty
)

func newBitfieldFromPieceStatusBytes(name string, raw []byte) *bitset.BitSet {
	bitfield := bitset.New(uint(len(raw)))
	for i, b := range raw {
		status := pieceStatus(b)
		if status != _empty && status != _complete {
			log.Errorf("Unexpected status at %d in piece metadata %s: %d", i, name, status)
			status = _empty
		}
		if status == _complete {
			bitfield.Set(uint(i))
		}
	}
	return bitfield
}

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
// expensive. Instead, Torrent tracks completed pieces on disk via metadata
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

	bitfield := newBitfieldFromPieceStatusBytes(name, raw)
	pieces = make([]*piece, numPieces)

	for i := 0; i < numPieces; i++ {
		if bitfield.Test(uint(i)) {
			pieces[i] = &piece{status: _complete}
		} else {
			pieces[i] = &piece{status: _empty}
		}
	}

	return pieces, int(bitfield.Count()), nil
}

// Torrent implements a Torrent on top of an AgentFileStore.
// It Allows concurrent writes on distinct pieces, and concurrent reads on all
// pieces. Behavior is undefined if multiple Torrent instances are backed
// by the same file store and metainfo.
type Torrent struct {
	metaInfo    *core.MetaInfo
	store       store.FileStore
	pieces      []*piece
	numComplete *atomic.Int32
}

// NewTorrent creates a new Torrent.
func NewTorrent(store store.FileStore, mi *core.MetaInfo) (*Torrent, error) {
	pieces, numComplete, err := restorePieces(mi.Name(), store, mi.Info.NumPieces())
	if err != nil {
		return nil, fmt.Errorf("restore pieces: %s", err)
	}

	if numComplete == len(pieces) {
		if err := store.MoveDownloadFileToCache(mi.Name()); err != nil && !os.IsExist(err) {
			return nil, fmt.Errorf("move file to cache: %s", err)
		}
	}

	return &Torrent{
		store:       store,
		metaInfo:    mi,
		pieces:      pieces,
		numComplete: atomic.NewInt32(int32(numComplete)),
	}, nil
}

// Name returns the name of the target file.
func (t *Torrent) Name() string {
	return t.metaInfo.Info.Name
}

// Stat returns the storage.TorrentInfo for t.
func (t *Torrent) Stat() *storage.TorrentInfo {
	return storage.NewTorrentInfo(t.metaInfo, t.Bitfield())
}

// InfoHash returns the torrent metainfo hash.
func (t *Torrent) InfoHash() core.InfoHash {
	return t.metaInfo.InfoHash
}

// NumPieces returns the number of pieces in the torrent.
func (t *Torrent) NumPieces() int {
	return len(t.pieces)
}

// Length returns the length of the target file.
func (t *Torrent) Length() int64 {
	return t.metaInfo.Info.Length
}

// PieceLength returns the length of piece pi.
func (t *Torrent) PieceLength(pi int) int64 {
	return t.metaInfo.Info.GetPieceLength(pi)
}

// MaxPieceLength returns the longest piece length of the torrent.
func (t *Torrent) MaxPieceLength() int64 {
	return t.PieceLength(0)
}

// Complete indicates whether the torrent is complete or not.
func (t *Torrent) Complete() bool {
	return int(t.numComplete.Load()) == len(t.pieces)
}

// BytesDownloaded returns an estimate of the number of bytes downloaded in the
// torrent.
func (t *Torrent) BytesDownloaded() int64 {
	return min(int64(t.numComplete.Load())*t.metaInfo.Info.PieceLength, t.metaInfo.Info.Length)
}

// Bitfield returns the bitfield of pieces where true denotes a complete piece
// and false denotes an incomplete piece.
func (t *Torrent) Bitfield() *bitset.BitSet {
	bitfield := bitset.New(uint(len(t.pieces)))
	for i, p := range t.pieces {
		if p.complete() {
			bitfield.Set(uint(i))
		}
	}
	return bitfield
}

func (t *Torrent) String() string {
	downloaded := int(float64(t.BytesDownloaded()) / float64(t.metaInfo.Info.Length) * 100)
	return fmt.Sprintf(
		"torrent(name=%s, hash=%s, downloaded=%d%%)",
		t.Name(), t.InfoHash().HexString(), downloaded)
}

func (t *Torrent) getPiece(pi int) (*piece, error) {
	if pi >= len(t.pieces) {
		return nil, fmt.Errorf("invalid piece index %d: num pieces = %d", pi, len(t.pieces))
	}
	return t.pieces[pi], nil
}

// markPieceComplete must only be called once per piece.
func (t *Torrent) markPieceComplete(pi int) error {
	updated, err := t.store.States().Download().SetMetadataAt(
		t.Name(), store.NewPieceStatus(), []byte{byte(_complete)}, pi)
	if err != nil {
		return fmt.Errorf("write piece metadata: %s", err)
	}
	if !updated {
		// This could mean there's another thread with a Torrent instance using
		// the same file as us.
		log.Errorf("Invariant violation: piece marked complete twice: piece %d in %s", pi, t.Name())
	}
	t.pieces[pi].markComplete()
	t.numComplete.Inc()
	return nil
}

// writePiece writes data to piece pi. If the write succeeds, marks the piece as completed.
func (t *Torrent) writePiece(src storage.PieceReader, pi int) error {
	f, err := t.store.GetDownloadFileReadWriter(t.metaInfo.Info.Name)
	if err != nil {
		return fmt.Errorf("get download writer: %s", err)
	}
	defer f.Close()

	h := core.PieceHash()
	r := io.TeeReader(src, h) // Calculates piece sum as we write to file.

	if _, err := f.Seek(t.getFileOffset(pi), 0); err != nil {
		return fmt.Errorf("seek: %s", err)
	}
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("copy: %s", err)
	}
	if h.Sum32() != t.metaInfo.Info.PieceSums[pi] {
		return errors.New("invalid piece sum")
	}

	if err := t.markPieceComplete(pi); err != nil {
		return fmt.Errorf("mark piece complete: %s", err)
	}
	return nil
}

// WritePiece writes data to piece pi.
func (t *Torrent) WritePiece(src storage.PieceReader, pi int) error {
	piece, err := t.getPiece(pi)
	if err != nil {
		return err
	}
	if int64(src.Length()) != t.PieceLength(pi) {
		return fmt.Errorf(
			"invalid piece length: expected %d, got %d", t.PieceLength(pi), src.Length())
	}

	// Exit quickly if the piece is not writable.
	if piece.complete() {
		return storage.ErrPieceComplete
	}
	if piece.dirty() {
		return errWritePieceConflict
	}

	dirty, complete := piece.tryMarkDirty()
	if dirty {
		return errWritePieceConflict
	} else if complete {
		return storage.ErrPieceComplete
	}

	// At this point, we've determined that the piece is not complete and ensured
	// we are the only thread which may write the piece. We do not block other
	// threads from checking if the piece is writable.

	if err := t.writePiece(src, pi); err != nil {
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

type opener struct {
	torrent *Torrent
}

func (o *opener) Open() (store.FileReader, error) {
	return o.torrent.store.GetDownloadOrCacheFileReader(o.torrent.Name())
}

// GetPieceReader returns a reader for piece pi.
func (t *Torrent) GetPieceReader(pi int) (storage.PieceReader, error) {
	piece, err := t.getPiece(pi)
	if err != nil {
		return nil, err
	}
	if !piece.complete() {
		return nil, errors.New("piece not complete")
	}
	return piecereader.NewFileReader(t.getFileOffset(pi), t.PieceLength(pi), &opener{t}), nil
}

// HasPiece returns if piece pi is complete.
func (t *Torrent) HasPiece(pi int) bool {
	piece, err := t.getPiece(pi)
	if err != nil {
		return false
	}
	return piece.complete()
}

// MissingPieces returns the indeces of all missing pieces.
func (t *Torrent) MissingPieces() []int {
	var missing []int
	for i, p := range t.pieces {
		if !p.complete() {
			missing = append(missing, i)
		}
	}
	return missing
}

// getFileOffset calculates the offset in the torrent file given piece index.
// Assumes pi is a valid piece index.
func (t *Torrent) getFileOffset(pi int) int64 {
	return t.metaInfo.Info.PieceLength * int64(pi)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
