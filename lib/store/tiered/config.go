package tiered

import (
	"errors"

	"github.com/uber/kraken/lib/store/disk"
	"github.com/uber/kraken/lib/store/memory"
)

const _defaultNumFlushWorkers = 10

// Config configures [Store].
type Config struct {
	DiskConfig      *disk.Config   `yaml:"disk_store"`
	MemConfig       *memory.Config `yaml:"memory_store"`
	NumFlushWorkers int            `yaml:"num_flush_workers"`
	// TODO - support disabling mem layer through the config to support quick incident mitigation.
}

func (c *Config) applyDefaults() error {
	if c.DiskConfig.RebootIncompleteBlobs {
		return errors.New("tiered.Store does not support RebootIncompleteBlobs, as it can leak files. Use disk.Store if you need persistence for incomplete blobs")
	}
	if c.NumFlushWorkers < 0 {
		return errors.New("num_flush_workers must be at least 1, otherwise blobs will never get flushed from mem to disk")
	}
	if c.NumFlushWorkers == 0 {
		c.NumFlushWorkers = _defaultNumFlushWorkers
	}
	return nil
}
