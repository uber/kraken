package main

import (
	"code.uber.internal/infra/kraken/build-index/tagserver"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/metrics"
	"go.uber.org/zap"
)

// Config defines build-index configuration.
type Config struct {
	ZapLogging     zap.Config                  `yaml:"zap"`
	Metrics        metrics.Config              `yaml:"metrics"`
	Namespaces     backend.NamespaceConfig     `yaml:"namespaces"`
	AuthNamespaces backend.AuthNamespaceConfig `yaml:"auth"`
	TagServer      tagserver.Config            `yaml:"tagserver"`
	Port           int                         `yaml:"port"`
}
