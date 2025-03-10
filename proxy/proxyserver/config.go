package proxyserver

import "github.com/uber/kraken/utils/listener"

type Config struct {
	Listener  listener.Config `yaml:"listener"`
	Threshold int64           `yaml:"threshold"`
}
