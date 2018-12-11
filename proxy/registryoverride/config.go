package registryoverride

import "github.com/uber/kraken/utils/listener"

// Config defines Server configuration.
type Config struct {
	Listener listener.Config `yaml:"listener"`
}
