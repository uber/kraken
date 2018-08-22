package main

import (
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/upstream"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/nginx"
	"code.uber.internal/infra/kraken/proxy/registryoverride"
)

// Config defines proxy configuration
type Config struct {
	CAStore          store.CAStoreConfig     `yaml:"castore"`
	Registry         dockerregistry.Config   `yaml:"registry"`
	BuildIndex       string                  `yaml:"build_index"`
	Origin           upstream.Config         `yaml:"origin"`
	ZapLogging       zap.Config              `yaml:"zap"`
	Metrics          metrics.Config          `yaml:"metrics"`
	RegistryOverride registryoverride.Config `yaml:"registryoverride"`
	Nginx            nginx.Config            `yaml:"nginx"`
}
