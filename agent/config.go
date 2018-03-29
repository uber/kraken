package main

import (
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/agent/agentserver"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent/networkevent"
	"code.uber.internal/infra/kraken/lib/torrent/scheduler"
	"code.uber.internal/infra/kraken/metrics"
)

// Config defines agent configuration.
type Config struct {
	ZapLogging     zap.Config                  `yaml:"zap"`
	Metrics        metrics.Config              `yaml:"metrics"`
	Store          store.Config                `yaml:"store"`
	Registry       dockerregistry.Config       `yaml:"registry"`
	Scheduler      scheduler.Config            `yaml:"scheduler"`
	PeerIDFactory  core.PeerIDFactory          `yaml:"peer_id_factory"`
	NetworkEvent   networkevent.Config         `yaml:"network_event"`
	Tracker        TrackerConfig               `yaml:"tracker"`
	AgentServer    agentserver.Config          `yaml:"agentserver"`
	AuthNamespaces backend.AuthNamespaceConfig `yaml:"auth"`
}

// TrackerConfig defines configuration for agent's dependency on tracker.
type TrackerConfig struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`
}
