package storage

import (
	"io"

	"code.uber.internal/infra/kraken/torlib"
)

// Torrent represents a read/write interface for a torrent
type Torrent interface {
	io.ReaderAt
	io.WriterAt
}

// TorrentManager represents data storage for torrent
type TorrentManager interface {
	CreateTorrent(infoHash torlib.InfoHash, infoBytes []byte) (Torrent, error)
	OpenTorrent(infoHash torlib.InfoHash) (Torrent, []byte, error)
	Close() error
}
