package storage

import (
	"io"

	"code.uber.internal/infra/kraken/client/torrent/meta"
)

// Torrent represents a read/write interface for a torrent
type Torrent interface {
	io.ReaderAt
	io.WriterAt
}

// TorrentManager represents data storage for torrent
type TorrentManager interface {
	CreateTorrent(infoHash meta.Hash, infoBytes []byte) (Torrent, error)
	OpenTorrent(infoHash meta.Hash) (Torrent, []byte, error)
	Close() error
}
