package service

import "time"

// Config defines configuration for the tracker service.
type Config struct {
	AnnounceInterval time.Duration  `yaml:"announce_interval"`
	MetaInfo         MetaInfoConfig `yaml:"metainfo"`
}
