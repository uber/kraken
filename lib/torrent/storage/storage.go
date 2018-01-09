package storage

import (
	"errors"

	"code.uber.internal/infra/kraken/torlib"
)

// TorrentArchive errors.
var (
	ErrNotFound = errors.New("torrent not found")
)

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
	Bitfield() Bitfield
	String() string

	HasPiece(piece int) bool
	MissingPieces() []int

	WritePiece(data []byte, piece int) error
	ReadPiece(piece int) ([]byte, error)
}

// TorrentArchive creates and open torrent file
type TorrentArchive interface {
	Stat(name string) (*TorrentInfo, error)
	CreateTorrent(namespace, name string) (Torrent, error)
	GetTorrent(name string) (Torrent, error)
	DeleteTorrent(name string) error
}
