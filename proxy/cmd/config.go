package cmd

import (
	"github.com/uber/kraken/lib/dockerregistry"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/lib/upstream"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/proxy/registryoverride"
	"github.com/uber/kraken/utils/httputil"

	"go.uber.org/zap"
)

// Config defines proxy configuration
type Config struct {
	CAStore          store.CAStoreConfig     `yaml:"castore"`
	Registry         dockerregistry.Config   `yaml:"registry"`
	BuildIndex       upstream.ActiveConfig   `yaml:"build_index"`
	Origin           upstream.ActiveConfig   `yaml:"origin"`
	ZapLogging       zap.Config              `yaml:"zap"`
	Metrics          metrics.Config          `yaml:"metrics"`
	RegistryOverride registryoverride.Config `yaml:"registryoverride"`
	Nginx            nginx.Config            `yaml:"nginx"`
	TLS              httputil.TLSConfig      `yaml:"tls"`
}
