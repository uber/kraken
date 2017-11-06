package transfer

import "code.uber.internal/infra/kraken/utils/memsize"

//OriginClusterTransfererConfig defines configuration for the OriginClusterTransferer.
type OriginClusterTransfererConfig struct {

	// Concurrency defines the number of concurrent downloads and uploads allowed.
	Concurrency int `yaml:"concurrency"`

	// TorrentPieceLength defines the piece length of torrents created for
	// uploaded blobs.
	TorrentPieceLength int64 `yaml:"torrent_piece_length"`
}

func (c OriginClusterTransfererConfig) applyDefaults() OriginClusterTransfererConfig {
	if c.Concurrency == 0 {
		c.Concurrency = 800
	}
	if c.TorrentPieceLength == 0 {
		c.TorrentPieceLength = int64(256 * memsize.KB)
	}
	return c
}
