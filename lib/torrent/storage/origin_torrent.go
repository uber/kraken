package storage

import (
	"errors"
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"

	"github.com/willf/bitset"
	"go.uber.org/atomic"
)

// OriginTorrentErrors
var (
	ErrReadOnly = errors.New("Read-only torrent is being written to")
)

// OriginTorrent implements a read-only Torrent on top of OriginFileStore.
// It allows concurrent reads on all pieces. Behavior is undefined if multiple
// OriginTorrent instances are backed by the same file store and metainfo.
type OriginTorrent struct {
	metaInfo    *core.MetaInfo
	store       store.OriginFileStore
	pieces      []*piece
	numComplete *atomic.Int32
}

// NewOriginTorrent creates a new OriginTorrent.
func NewOriginTorrent(store store.OriginFileStore, mi *core.MetaInfo) (*OriginTorrent, error) {
	// For OriginTorrent, all pieces are already completed.
	pieces := make([]*piece, mi.Info.NumPieces())
	for i := range pieces {
		pieces[i] = &piece{status: _complete}
	}
	return &OriginTorrent{
		store:       store,
		metaInfo:    mi,
		pieces:      pieces,
		numComplete: atomic.NewInt32(int32(mi.Info.NumPieces())),
	}, nil
}

// Name returns the name of the target file.
func (t *OriginTorrent) Name() string {
	return t.metaInfo.Info.Name
}

// Stat returns the TorrentInfo for t.
func (t *OriginTorrent) Stat() *TorrentInfo {
	return newTorrentInfo(t.metaInfo, t.Bitfield())
}

// InfoHash returns the torrent metainfo hash.
func (t *OriginTorrent) InfoHash() core.InfoHash {
	return t.metaInfo.InfoHash
}

// NumPieces returns the number of pieces in the torrent.
func (t *OriginTorrent) NumPieces() int {
	return len(t.pieces)
}

// Length returns the length of the target file.
func (t *OriginTorrent) Length() int64 {
	return t.metaInfo.Info.Length
}

// PieceLength returns the length of piece pi.
func (t *OriginTorrent) PieceLength(pi int) int64 {
	if pi == len(t.pieces)-1 {
		// Last piece.
		return t.Length() - t.metaInfo.Info.PieceLength*int64(pi)
	}
	return t.metaInfo.Info.PieceLength
}

// MaxPieceLength returns the longest piece length of the torrent.
func (t *OriginTorrent) MaxPieceLength() int64 {
	return t.PieceLength(0)
}

// Complete indicates whether the torrent is complete or not.
func (t *OriginTorrent) Complete() bool {
	return int(t.numComplete.Load()) == len(t.pieces)
}

// BytesDownloaded returns an estimate of the number of bytes downloaded in the
// torrent. For OriginTorrent, it's always the total number of pieces.
func (t *OriginTorrent) BytesDownloaded() int64 {
	return t.metaInfo.Info.Length
}

// WritePiece returns error, since OriginTorrent is read-only.
func (t *OriginTorrent) WritePiece(src PieceReader, pi int) error {
	return ErrReadOnly
}

// Bitfield returns the bitfield of pieces where true denotes a complete piece
// and false denotes an incomplete piece. For OriginTorrent it's always true.
func (t *OriginTorrent) Bitfield() *bitset.BitSet {
	return bitset.New(uint(len(t.pieces))).Complement()
}

func (t *OriginTorrent) String() string {
	downloaded := int(float64(t.BytesDownloaded()) / float64(t.metaInfo.Info.Length) * 100)
	return fmt.Sprintf("torrent(hash=%s, downloaded=%d%%)", t.InfoHash().HexString(), downloaded)
}

// GetPieceReader returns a reader for piece pi.
func (t *OriginTorrent) GetPieceReader(pi int) (PieceReader, error) {
	if pi >= len(t.pieces) {
		return nil, fmt.Errorf("invalid piece index %d: num pieces = %d", pi, len(t.pieces))
	}
	return newFilePieceReader(t.getFileOffset(pi), t.PieceLength(pi), t.openFile), nil
}

func (t *OriginTorrent) openFile() (store.FileReader, error) {
	return t.store.GetCacheFileReader(t.metaInfo.Info.Name)
}

// HasPiece returns if piece pi is complete.
// For OriginTorrent it's always true.
func (t *OriginTorrent) HasPiece(pi int) bool {
	return true
}

// MissingPieces returns the indices of all missing pieces.
// For OriginTorrent it should be an empty list.
func (t *OriginTorrent) MissingPieces() []int {
	return []int{}
}

// getFileOffset calculates the offset in the torrent file given piece index.
// Assumes pi is a valid piece index.
func (t *OriginTorrent) getFileOffset(pi int) int64 {
	return t.metaInfo.Info.PieceLength * int64(pi)
}
