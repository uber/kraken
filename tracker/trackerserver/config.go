package trackerserver

import (
	"time"

	"code.uber.internal/infra/kraken/utils/dedup"
)

// Config defines configuration for the tracker service.
type Config struct {
	MetaInfoRequestCache dedup.RequestCacheConfig `yaml:"metainfo_request_cache"`
	TagCache             dedup.CacheConfig        `yaml:"tag_cache"`

	// Limits the number of peers returned on each announce.
	PeerHandoutLimit int `yaml:"announce_limit"`

	AnnounceInterval time.Duration `yaml:"announce_interval"`
}

func (c Config) applyDefaults() Config {
	if c.PeerHandoutLimit == 0 {
		c.PeerHandoutLimit = 50
	}
	if c.AnnounceInterval == 0 {
		c.AnnounceInterval = 3 * time.Second
	}
	return c
}
