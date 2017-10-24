package main

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/serverset"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobclient"
)

// Config defines proxy configuration
type Config struct {
	Tracker     TrackerConfig `yaml:"tracker"`
	Origin      OriginConfig  `yaml:"origin"`
	Concurrency int           `yaml:"concurrency"`

	Store    store.Config          `yaml:"store"`
	Registry dockerregistry.Config `yaml:"registry"`
	Logging  log.Configuration     `yaml:"logging"`
	Metrics  metrics.Config        `yaml:"metrics"`
}

// OriginConfig defines configuration for proxy's dependency on the origin cluster.
type OriginConfig struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`
	Client     blobclient.Config          `yaml:"client"`
}

// TrackerConfig defines configuration for proxy's dependency on tracker.
type TrackerConfig struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`
}
