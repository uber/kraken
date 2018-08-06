package originstore

import "time"

// Config defines Store configuration.
type Config struct {
	LocationsTTL         time.Duration `yaml:"locations_ttl"`
	LocationsErrorTTL    time.Duration `yaml:"locations_error_ttl"`
	OriginContextTTL     time.Duration `yaml:"origin_context_ttl"`
	OriginUnavailableTTL time.Duration `yaml:"origin_unavailable_ttl"`
}

func (c *Config) applyDefaults() {
	if c.LocationsTTL == 0 {
		c.LocationsTTL = 10 * time.Second
	}
	if c.LocationsErrorTTL == 0 {
		c.LocationsErrorTTL = time.Second
	}
	if c.OriginContextTTL == 0 {
		c.OriginContextTTL = 10 * time.Second
	}
	if c.OriginUnavailableTTL == 0 {
		c.OriginUnavailableTTL = time.Minute
	}
}
