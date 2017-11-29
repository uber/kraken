package main

import (
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
)

// Config defines proxy configuration
type Config struct {
	Tracker    TrackerConfig                          `yaml:"tracker"`
	Origin     OriginConfig                           `yaml:"origin"`
	Transfer   transfer.OriginClusterTransfererConfig `yaml:"transfer"`
	Store      store.Config                           `yaml:"store"`
	Registry   dockerregistry.Config                  `yaml:"registry"`
	ZapLogging zap.Config                             `yaml:"zap"`
	Metrics    metrics.Config                         `yaml:"metrics"`
}

// OriginConfig defines configuration for proxy's dependency on the origin cluster.
type OriginConfig struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`
	Client     blobclient.Config          `yaml:"client"`
}

// TrackerConfig defines configuration for proxy's dependency on tracker.
type TrackerConfig struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`
}
