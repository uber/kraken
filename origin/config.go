package main

import (
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/blobrefresh"
	"code.uber.internal/infra/kraken/lib/metainfogen"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/localdb"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobserver"
)

// Config defines origin server configuration.
type Config struct {
	Verbose       bool
	ZapLogging    zap.Config            `yaml:"zap"`
	BlobServer    blobserver.Config     `yaml:"blobserver"`
	CAStore       store.CAStoreConfig   `yaml:"castore"`
	Scheduler     scheduler.Config      `yaml:"scheduler"`
	NetworkEvent  networkevent.Config   `yaml:"network_event"`
	PeerIDFactory core.PeerIDFactory    `yaml:"peer_id_factory"`
	Metrics       metrics.Config        `yaml:"metrics"`
	MetaInfoGen   metainfogen.Config    `yaml:"metainfogen"`
	Backends      []backend.Config      `yaml:"backends"`
	Auth          backend.AuthConfig    `yaml:"auth"`
	BlobRefresh   blobrefresh.Config    `yaml:"blobrefresh"`
	LocalDB       localdb.Config        `yaml:"localdb"`
	WriteBack     persistedretry.Config `yaml:"write_back"`
}
