package storage

import (
	"errors"
	"io"

	"code.uber.internal/infra/kraken/torlib"

	"github.com/willf/bitset"
)

// TorrentArchive errors.
var (
	ErrNotFound = errors.New("torrent not found")
)

// PieceReader defines operations for lazy piece reading.
type PieceReader interface {
	io.ReadCloser
	Length() int
}

// Torrent represents a read/write interface for a torrent
type Torrent interface {
	// TODO(codyg): Move some of these methods into TorrentInfo.
	Name() string
	Stat() *TorrentInfo
	NumPieces() int
	Length() int64
	PieceLength(piece int) int64
	MaxPieceLength() int64
	InfoHash() torlib.InfoHash
	Complete() bool
	BytesDownloaded() int64
	Bitfield() *bitset.BitSet
	String() string

	HasPiece(piece int) bool
	MissingPieces() []int

	WritePiece(data []byte, piece int) error
	GetPieceReader(piece int) (PieceReader, error)
}

// TorrentArchive creates and open torrent file
type TorrentArchive interface {
	Stat(name string) (*TorrentInfo, error)
	CreateTorrent(namespace, name string) (Torrent, error)
	GetTorrent(name string) (Torrent, error)
	DeleteTorrent(name string) error
}
