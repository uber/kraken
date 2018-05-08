package main

import (
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/metrics"
)

// Config defines proxy configuration
type Config struct {
	Store      store.Config          `yaml:"store"`
	Registry   dockerregistry.Config `yaml:"registry"`
	BuildIndex string                `yaml:"build_index"`
	Origin     string                `yaml:"origin"`
	ZapLogging zap.Config            `yaml:"zap"`
	Metrics    metrics.Config        `yaml:"metrics"`
}
