package torrent

import "code.uber.internal/infra/kraken/client/torrent/scheduler"

// Config contains torrent client config
type Config struct {
	PeerID      string           `yaml:"peer_id"`
	PieceLength int64            `yaml:"piece_length"`
	Disabled    bool             `yaml:"disabled"`
	Scheduler   scheduler.Config `yaml:"scheduler"`
}
