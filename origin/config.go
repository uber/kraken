package main

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobserver"
)

// Config defines origin server configuration.
type Config struct {
	Logging    log.Configuration
	Port       int
	Verbose    bool
	BlobServer blobserver.Config
	LocalStore store.Config
}
