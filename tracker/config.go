package main

import (
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/storage"
	"code.uber.internal/infra/kraken/tracker/trackerserver"
)

// Config defines tracker configuration.
type Config struct {
	ZapLogging        zap.Config                  `yaml:"zap"`
	Port              int                         `yaml:"port"`
	Storage           storage.Config              `yaml:"storage"`
	TrackerServer     trackerserver.Config        `yaml:"trackerserver"`
	PeerHandoutPolicy peerhandoutpolicy.Config    `yaml:"peerhandoutpolicy"`
	Origin            OriginConfig                `yaml:"origin"`
	Metrics           metrics.Config              `yaml:"metrics"`
	Namespaces        backend.NamespaceConfig     `yaml:"namespaces"`
	AuthNamespaces    backend.AuthNamespaceConfig `yaml:"auth"`
	TagNamespace      string                      `yaml:"tag_namespace"`
}

// OriginConfig defines configuration for tracker's dependency on the
// origin cluster.
type OriginConfig struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`
}
