package registryoverride

import "code.uber.internal/infra/kraken/utils/listener"

// Config defines Server configuration.
type Config struct {
	Listener listener.Config `yaml:"listener"`
}
