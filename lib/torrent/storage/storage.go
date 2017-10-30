package storage

import "code.uber.internal/infra/kraken/torlib"

// Torrent represents a read/write interface for a torrent
type Torrent interface {
	Name() string
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
	CreateTorrent(mi *torlib.MetaInfo) (Torrent, error)
	GetTorrent(name string, infoHash torlib.InfoHash) (Torrent, error)
	DeleteTorrent(name string, infoHash torlib.InfoHash) error
	Close() error
}
