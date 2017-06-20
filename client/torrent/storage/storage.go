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

// TorrentStorage represents data storage for torrent
type TorrentStorage interface {
	OpenTorrent(info *meta.Info, infoHash meta.Hash) (Torrent, error)
	Close() error
}
