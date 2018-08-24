package main

import (
	"code.uber.internal/infra/kraken/build-index/tagserver"
	"code.uber.internal/infra/kraken/build-index/tagstore"
	"code.uber.internal/infra/kraken/build-index/tagtype"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/persistedretry"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/upstream"
	"code.uber.internal/infra/kraken/localdb"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/nginx"

	"go.uber.org/zap"
)

// Config defines build-index configuration.
type Config struct {
	ZapLogging     zap.Config                   `yaml:"zap"`
	Metrics        metrics.Config               `yaml:"metrics"`
	Backends       []backend.Config             `yaml:"backends"`
	Auth           backend.AuthConfig           `yaml:"auth"`
	TagServer      tagserver.Config             `yaml:"tagserver"`
	Remotes        tagreplication.RemotesConfig `yaml:"remotes"`
	TagReplication persistedretry.Config        `yaml:"tag_replication"`
	TagTypes       []tagtype.Config             `yaml:"tag_types"`
	Origin         upstream.ActiveConfig        `yaml:"origin"`
	LocalDB        localdb.Config               `yaml:"localdb"`
	Cluster        upstream.ActiveConfig        `yaml:"cluster"`
	TagStore       tagstore.Config              `yaml:"tag_store"`
	Store          store.SimpleStoreConfig      `yaml:"store"`
	WriteBack      persistedretry.Config        `yaml:"writeback"`
	Nginx          nginx.Config                 `yaml:"nginx"`
}
