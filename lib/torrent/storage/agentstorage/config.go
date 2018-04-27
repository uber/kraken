package agentstorage

import "time"

// Config defines TorrentArchive configuration.
type Config struct {
	UnavailableMetaInfoRetries    int
	UnavailableMetaInfoRetrySleep time.Duration
}

func (c Config) applyDefaults() Config {
	if c.UnavailableMetaInfoRetries == 0 {
		c.UnavailableMetaInfoRetries = 3
	}
	if c.UnavailableMetaInfoRetrySleep == 0 {
		c.UnavailableMetaInfoRetrySleep = 5 * time.Second
	}
	return c
}
