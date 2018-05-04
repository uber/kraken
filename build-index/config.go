package main

import (
	"code.uber.internal/infra/kraken/build-index/remotes"
	"code.uber.internal/infra/kraken/build-index/tagserver"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/metrics"
	"go.uber.org/zap"
)

// Config defines build-index configuration.
type Config struct {
	ZapLogging       zap.Config              `yaml:"zap"`
	Metrics          metrics.Config          `yaml:"metrics"`
	Namespaces       backend.NamespaceConfig `yaml:"namespaces"`
	Auth             backend.AuthConfig      `yaml:"auth"`
	TagServer        tagserver.Config        `yaml:"tagserver"`
	Remotes          remotes.Config          `yaml:"remotes"`
	Origin           string                  `yaml:"origin"`
	Port             int                     `yaml:"port"`
	SQLiteSourcePath string                  `yaml:"sqlite_source_path"`
}
