package main

import (
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/agent/agentserver"
	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
	"code.uber.internal/infra/kraken/metrics"
)

// Config defines agent configuration.
type Config struct {
	ZapLogging  zap.Config            `yaml:"zap"`
	Metrics     metrics.Config        `yaml:"metrics"`
	Store       store.Config          `yaml:"store"`
	Registry    dockerregistry.Config `yaml:"registry"`
	Torrent     torrent.Config        `yaml:"torrent"`
	Tracker     TrackerConfig         `yaml:"tracker"`
	AgentServer agentserver.Config    `yaml:"agentserver"`
}

// TrackerConfig defines configuration for agent's dependency on tracker.
type TrackerConfig struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`
}
