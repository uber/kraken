package torrent

import "code.uber.internal/infra/kraken/client/torrent/scheduler"

// Config contains torrent client config
type Config struct {
	PeerIDFactory string           `yaml:"peer_id_factory"`
	PieceLength   int64            `yaml:"piece_length"`
	Disabled      bool             `yaml:"disabled"`
	Scheduler     scheduler.Config `yaml:"scheduler"`
}
