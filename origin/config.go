package main

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/lib/torrent"
	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/origin/blobserver"
)

// Config defines origin server configuration.
type Config struct {
	Port       int
	Verbose    bool
	Logging    log.Configuration
	LocalStore store.Config      `yaml:"store"`
	Torrent    torrent.Config    `yaml:"torrent"`
	BlobServer blobserver.Config `yaml:"blobserver"`
	Metrics    metrics.Config    `yaml:"metrics"`
}
