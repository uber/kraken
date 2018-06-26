package tagstore

import "code.uber.internal/infra/kraken/utils/dedup"

// Config defines Store configuration.
type Config struct {
	Cache dedup.CacheConfig `yaml:"cache"`
}
