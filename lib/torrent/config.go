package torrent

import (
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
)

// Config contains torrent client config
type Config struct {
	PeerIDFactory string              `yaml:"peer_id_factory"`
	PieceLength   int64               `yaml:"piece_length"`
	Scheduler     scheduler.Config    `yaml:"scheduler"`
	NetworkEvent  networkevent.Config `yaml:"network_event"`
}
