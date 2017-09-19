package main

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/mysql"

	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/service"
	"code.uber.internal/infra/kraken/tracker/storage"
)

// Config defines tracker configuration.
type Config struct {
	Logging           log.Configuration        `yaml:"logging"`
	BackendPort       int                      `yaml:"backendport"`
	FrontendPort      int                      `yaml:"frontendport"`
	Storage           storage.Config           `yaml:"storage"`
	Service           service.Config           `yaml:"service"`
	PeerHandoutPolicy peerhandoutpolicy.Config `yaml:"peerhandoutpolicy"`
	// Unfortunately, nemo must be in top-level configuration to allow secrets
	// injection.
	Nemo mysql.Configuration `yaml:"nemo"`
}
