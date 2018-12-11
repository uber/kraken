package main

import (
	"go.uber.org/zap"

	"github.com/uber/kraken/lib/upstream"
	"github.com/uber/kraken/metrics"
	"github.com/uber/kraken/nginx"
	"github.com/uber/kraken/tracker/originstore"
	"github.com/uber/kraken/tracker/peerhandoutpolicy"
	"github.com/uber/kraken/tracker/peerstore"
	"github.com/uber/kraken/tracker/trackerserver"
	"github.com/uber/kraken/utils/httputil"
)

// Config defines tracker configuration.
type Config struct {
	ZapLogging        zap.Config               `yaml:"zap"`
	Port              int                      `yaml:"port"`
	PeerStore         peerstore.Config         `yaml:"peerstore"`
	OriginStore       originstore.Config       `yaml:"originstore"`
	TrackerServer     trackerserver.Config     `yaml:"trackerserver"`
	PeerHandoutPolicy peerhandoutpolicy.Config `yaml:"peerhandoutpolicy"`
	Origin            upstream.ActiveConfig    `yaml:"origin"`
	Metrics           metrics.Config           `yaml:"metrics"`
	Nginx             nginx.Config             `yaml:"nginx"`
	TLS               httputil.TLSConfig       `yaml:"tls"`
}
