package main

import (
	"code.uber.internal/infra/kraken/build-index/tagserver"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/metrics"
	"go.uber.org/zap"
)

// Config defines build-index configuration.
type Config struct {
	ZapLogging       zap.Config                   `yaml:"zap"`
	Metrics          metrics.Config               `yaml:"metrics"`
	Backends         []backend.Config             `yaml:"backends"`
	Auth             backend.AuthConfig           `yaml:"auth"`
	TagServer        tagserver.Config             `yaml:"tagserver"`
	Remotes          tagreplication.RemotesConfig `yaml:"remotes"`
	TagReplication   persistedretry.Config        `yaml:"tag_replication"`
	Origin           string                       `yaml:"origin"`
	Port             int                          `yaml:"port"`
	SQLiteSourcePath string                       `yaml:"sqlite_source_path"`
}
