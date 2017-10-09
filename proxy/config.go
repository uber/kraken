package main

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/dockerregistry"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/metrics"
)

// Config defines proxy configuration
type Config struct {
	TrackAddr   string `yaml:"tracker_address"`
	OriginAddr  string `yaml:"origin_address"`
	Concurrency int    `yaml:"concurrency"`

	Store    store.Config          `yaml:"store"`
	Registry dockerregistry.Config `yaml:"registry"`
	Logging  log.Configuration     `yaml:"logging"`
	Metrics  metrics.Config        `yaml:"metrics"`
}
