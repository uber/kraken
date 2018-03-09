package trackerserver

import "code.uber.internal/infra/kraken/utils/dedup"

// Config defines configuration for the tracker service.
type Config struct {
	MetaInfoRequestCache dedup.RequestCacheConfig `yaml:"metainfo_request_cache"`
	TagCache             dedup.CacheConfig        `yaml:"tag_cache"`

	// Limits the number of peers returned on each announce.
	PeerHandoutLimit int `yaml:"announce_limit"`
}

func (c Config) applyDefaults() Config {
	if c.PeerHandoutLimit == 0 {
		c.PeerHandoutLimit = 50
	}
	return c
}
