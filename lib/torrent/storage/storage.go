package storage

import (
	"errors"
	"io"

	"code.uber.internal/infra/kraken/core"

	"github.com/willf/bitset"
)

// ErrNotFound occurs when TorrentArchive cannot found a torrent.
var ErrNotFound = errors.New("torrent not found")

// ErrPieceComplete occurs when Torrent cannot write a piece because it is already
// complete.
var ErrPieceComplete = errors.New("piece is already complete")

// PieceReader defines operations for lazy piece reading.
type PieceReader interface {
	io.ReadCloser
	Length() int
}

// Torrent represents a read/write interface for a torrent
type Torrent interface {
	Digest() core.Digest
	Stat() *TorrentInfo
	NumPieces() int
	Length() int64
	PieceLength(piece int) int64
	MaxPieceLength() int64
	InfoHash() core.InfoHash
	Complete() bool
	BytesDownloaded() int64
	Bitfield() *bitset.BitSet
	String() string

	HasPiece(piece int) bool
	MissingPieces() []int

	WritePiece(src PieceReader, piece int) error
	GetPieceReader(piece int) (PieceReader, error)
}

// TorrentArchive creates and open torrent file
type TorrentArchive interface {
	Stat(namespace string, d core.Digest) (*TorrentInfo, error)
	CreateTorrent(namespace string, d core.Digest) (Torrent, error)
	GetTorrent(namespace string, d core.Digest) (Torrent, error)
	DeleteTorrent(d core.Digest) error
}
