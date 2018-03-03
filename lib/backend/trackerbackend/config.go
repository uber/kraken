package trackerbackend

import "code.uber.internal/infra/kraken/lib/serverset"

// Config defines tracker backend configuration.
type Config struct {
	RoundRobin serverset.RoundRobinConfig `yaml:"round_robin"`
}
