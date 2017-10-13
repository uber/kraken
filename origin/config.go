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
	Verbose    bool
	Logging    log.Configuration
	BlobServer blobserver.Config       `yaml:"blobserver"`
	BlobClient blobserver.ClientConfig `yaml:"blobclient"`
	LocalStore store.Config            `yaml:"store"`
	Torrent    torrent.Config          `yaml:"torrent"`
	Metrics    metrics.Config          `yaml:"metrics"`
}
