package storage

import "code.uber.internal/infra/kraken/torlib"

// Torrent represents a read/write interface for a torrent
type Torrent interface {
	// Information
	Name() string
	NumPieces() int
	Length() int64
	PieceLength(piece int) int64
	InfoHash() torlib.InfoHash
	Complete() bool
	BytesDownloaded() int64
	Bitfield() Bitfield
	String() string

	// Piece operations
	HasPiece(piece int) bool
	MissingPieces() []int
	// Read/Write
	WritePiece(data []byte, piece int) (int, error)
	ReadPiece(piece int) ([]byte, error)
}

// TorrentArchive creates and open torrent file
type TorrentArchive interface {
	CreateTorrent(infoHash torlib.InfoHash, mi *torlib.MetaInfo) (Torrent, error)
	GetTorrent(name string, infoHash torlib.InfoHash) (Torrent, error)
	DeleteTorrent(name string, infoHash torlib.InfoHash) error
	Close() error
}
