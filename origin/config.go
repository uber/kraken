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
	Logging    log.Configuration
	Port       int
	Verbose    bool
	BlobServer blobserver.Config `yaml:"blobserver"`
	LocalStore store.Config      `yaml:"store"`
	Metrics    metrics.Config    `yaml:"metrics"`
	Torrent    torrent.Config    `yaml:"torrent"`
}
