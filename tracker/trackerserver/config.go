package trackerserver

import (
	"time"

	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/listener"
)

// Config defines configuration for the tracker service.
type Config struct {
	TagCache dedup.CacheConfig `yaml:"tag_cache"`

	// Limits the number of unique metainfo requests to origin per namespace/digest.
	GetMetaInfoLimit time.Duration `yaml:"get_metainfo_limit"`

	// Limits the number of peers returned on each announce.
	PeerHandoutLimit int `yaml:"announce_limit"`

	AnnounceInterval time.Duration `yaml:"announce_interval"`

	Listener listener.Config `yaml:"listener"`
}

func (c Config) applyDefaults() Config {
	if c.GetMetaInfoLimit == 0 {
		c.GetMetaInfoLimit = time.Second
	}
	if c.PeerHandoutLimit == 0 {
		c.PeerHandoutLimit = 50
	}
	if c.AnnounceInterval == 0 {
		c.AnnounceInterval = 3 * time.Second
	}
	return c
}
