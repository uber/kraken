package main

import (
	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/kraken/client/dockerregistry"
	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrent"
	"code.uber.internal/infra/kraken/metrics"
)

// Config defines agent configuration.
type Config struct {
	Logging  log.Configuration     `yaml:"logging"`
	Metrics  metrics.Config        `yaml:"metrics"`
	Store    store.Config          `yaml:"store"`
	Registry dockerregistry.Config `yaml:"registry"`
	Torrent  torrent.Config        `yaml:"torrent"`
}
