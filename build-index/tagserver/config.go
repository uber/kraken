package tagserver

import "code.uber.internal/infra/kraken/utils/dedup"

// Config defines Server configuration.
type Config struct {
	Cache dedup.CacheConfig `yaml:"cache"`
}
