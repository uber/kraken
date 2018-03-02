package main

import (
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobserver"
)

// Config defines origin server configuration.
type Config struct {
	Verbose        bool
	ZapLogging     zap.Config                  `yaml:"zap"`
	BlobServer     blobserver.Config           `yaml:"blobserver"`
	OriginStore    store.OriginConfig          `yaml:"originstore"`
	Torrent        torrent.Config              `yaml:"torrent"`
	Metrics        metrics.Config              `yaml:"metrics"`
	Tracker        TrackerConfig               `yaml:"tracker"`
	Namespaces     backend.NamespaceConfig     `yaml:"namespaces"`
	AuthNamespaces backend.AuthNamespaceConfig `yaml:"auth"`
}

// TrackerConfig defines configuration for proxy's dependency on tracker.
type TrackerConfig struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`
}
