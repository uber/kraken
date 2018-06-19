package originstorage

import (
	"errors"
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/storage"
	"code.uber.internal/infra/kraken/lib/torrent/storage/piecereader"

	"github.com/willf/bitset"
	"go.uber.org/atomic"
)

// Torrent errors.
var (
	ErrReadOnly = errors.New("Read-only torrent is being written to")
)

// Torrent is a read-only storage.Torrent. It allows concurrent reads on all
// pieces.
type Torrent struct {
	metaInfo    *core.MetaInfo
	cas         *store.CAStore
	numComplete *atomic.Int32
}

// NewTorrent creates a new Torrent.
func NewTorrent(cas *store.CAStore, mi *core.MetaInfo) (*Torrent, error) {
	return &Torrent{
		cas:         cas,
		metaInfo:    mi,
		numComplete: atomic.NewInt32(int32(mi.Info.NumPieces())),
	}, nil
}

// Name returns the name of the target file.
func (t *Torrent) Name() string {
	return t.metaInfo.Info.Name
}

// Stat returns the TorrentInfo for t.
func (t *Torrent) Stat() *storage.TorrentInfo {
	return storage.NewTorrentInfo(t.metaInfo, t.Bitfield())
}

// InfoHash returns the torrent metainfo hash.
func (t *Torrent) InfoHash() core.InfoHash {
	return t.metaInfo.InfoHash
}

// NumPieces returns the number of pieces in the torrent.
func (t *Torrent) NumPieces() int {
	return t.metaInfo.Info.NumPieces()
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

// Complete is always true.
func (t *Torrent) Complete() bool {
	return true
}

// BytesDownloaded always returns the total number of bytes.
func (t *Torrent) BytesDownloaded() int64 {
	return t.metaInfo.Info.Length
}

// WritePiece returns error, since Torrent is read-only.
func (t *Torrent) WritePiece(src storage.PieceReader, pi int) error {
	return ErrReadOnly
}

// Bitfield always returns a completed bitfield.
func (t *Torrent) Bitfield() *bitset.BitSet {
	return bitset.New(uint(t.NumPieces())).Complement()
}

func (t *Torrent) String() string {
	downloaded := int(float64(t.BytesDownloaded()) / float64(t.metaInfo.Info.Length) * 100)
	return fmt.Sprintf("torrent(hash=%s, downloaded=%d%%)", t.InfoHash().HexString(), downloaded)
}

type opener struct {
	torrent *Torrent
}

func (o *opener) Open() (store.FileReader, error) {
	return o.torrent.cas.GetCacheFileReader(o.torrent.Name())
}

// GetPieceReader returns a reader for piece pi.
func (t *Torrent) GetPieceReader(pi int) (storage.PieceReader, error) {
	if pi >= t.NumPieces() {
		return nil, fmt.Errorf("invalid piece index %d: num pieces = %d", pi, t.NumPieces())
	}
	return piecereader.NewFileReader(t.getFileOffset(pi), t.PieceLength(pi), &opener{t}), nil
}

// HasPiece returns if piece pi is complete.
// For Torrent it's always true.
func (t *Torrent) HasPiece(pi int) bool {
	return true
}

// MissingPieces always returns empty list.
func (t *Torrent) MissingPieces() []int {
	return []int{}
}

// getFileOffset calculates the offset in the torrent file given piece index.
// Assumes pi is a valid piece index.
func (t *Torrent) getFileOffset(pi int) int64 {
	return t.metaInfo.Info.PieceLength * int64(pi)
}
