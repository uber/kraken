package main

import (
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/agent/agentserver"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/lib/upstream"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/nginx"
)

// Config defines agent configuration.
type Config struct {
	ZapLogging      zap.Config                  `yaml:"zap"`
	Metrics         metrics.Config              `yaml:"metrics"`
	CADownloadStore store.CADownloadStoreConfig `yaml:"store"`
	Registry        dockerregistry.Config       `yaml:"registry"`
	Scheduler       scheduler.Config            `yaml:"scheduler"`
	PeerIDFactory   core.PeerIDFactory          `yaml:"peer_id_factory"`
	NetworkEvent    networkevent.Config         `yaml:"network_event"`
	Tracker         upstream.PassiveConfig      `yaml:"tracker"`
	BuildIndex      upstream.PassiveConfig      `yaml:"build_index"`
	AgentServer     agentserver.Config          `yaml:"agentserver"`
	RegistryBackup  string                      `yaml:"registry_backup"`
	Nginx           nginx.Config                `yaml:"nginx"`
}
