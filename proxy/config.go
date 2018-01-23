package main

import (
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/metrics"
)

// Config defines proxy configuration
type Config struct {
	Tracker    TrackerConfig           `yaml:"tracker"`
	Origin     OriginConfig            `yaml:"origin"`
	Store      store.Config            `yaml:"store"`
	Registry   dockerregistry.Config   `yaml:"registry"`
	ZapLogging zap.Config              `yaml:"zap"`
	Metrics    metrics.Config          `yaml:"metrics"`
	Namespaces backend.NamespaceConfig `yaml:"namespaces"`
	Namespace  string                  `yaml:"namespace"`
}

// OriginConfig defines configuration for proxy's dependency on the origin cluster.
type OriginConfig struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`
}

// TrackerConfig defines configuration for proxy's dependency on tracker.
type TrackerConfig struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`
}
