package main

import (
	"go.uber.org/zap"

	"code.uber.internal/infra/kraken/metrics"
	"code.uber.internal/infra/kraken/nginx"
	"code.uber.internal/infra/kraken/tracker/peerhandoutpolicy"
	"code.uber.internal/infra/kraken/tracker/peerstore"
	"code.uber.internal/infra/kraken/tracker/trackerserver"
)

// Config defines tracker configuration.
type Config struct {
	ZapLogging        zap.Config               `yaml:"zap"`
	Port              int                      `yaml:"port"`
	PeerStore         peerstore.Config         `yaml:"peerstore"`
	TrackerServer     trackerserver.Config     `yaml:"trackerserver"`
	PeerHandoutPolicy peerhandoutpolicy.Config `yaml:"peerhandoutpolicy"`
	Origin            string                   `yaml:"origin"`
	Metrics           metrics.Config           `yaml:"metrics"`
	Nginx             nginx.Config             `yaml:"nginx"`
}
