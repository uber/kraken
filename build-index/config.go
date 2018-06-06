package main

import (
	"code.uber.internal/infra/kraken/build-index/tagserver"
	"code.uber.internal/infra/kraken/build-index/tagtype"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/hostlist"
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
	TagTypes         []tagtype.Config             `yaml:"tag_types"`
	Origin           string                       `yaml:"origin"`
	SQLiteSourcePath string                       `yaml:"sqlite_source_path"`
	LocalReplicas    hostlist.Config              `yaml:"local_replicas"`
}
