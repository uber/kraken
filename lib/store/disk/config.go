package disk

import "errors"

const _defaultRootDir = "disk_store"

// Config configures [Store].
type Config struct {
	// Capacity is a *soft* limit for the capacity of the store. When breached, LRU eviction is used.
	// The limit is soft, as deleting a file on Linux removes it from disk only after all FDs to it are closed.
	// The store supports both weighted and unweighted eviction. An example of weighted eviction is to set capacity
	// to some number of bytes and provide the size of each blob in [Store.Create]. To use the store unweighted, set
	// capacity to the number of blobs and provide `size==1` in [Store.Create] for each blob.
	Capacity uint64 `yaml:"capacity"`
	// The root directory under which [Store]'s blobs and state are stored.
	RootDir string `yaml:"root_dir"`
	// Whether after crash/restart, the Store removes incomplete files from disk (usually to prevent leaks) OR
	// reboots any incomplete files from disk (allowing users to continue the blob download/upload, where it was left off before the crash).
	RebootIncompleteBlobs bool `yaml:"reboot_incomplete_blobs"`
	// If > 0, directory sharding is used to speed up performance, where ShardLength denotes
	// 1) the length of each directory shard's name and 2) the number of shards.
	// A value of 0 denotes no sharding.
	ShardLength int `yaml:"shard_length"`
}

func (c *Config) applyDefaults() error {
	if c.Capacity == 0 {
		return errors.New("capacity must be explicitly set")
	}
	if c.ShardLength < 0 {
		return errors.New("shard_length must be non-negative")
	}
	if c.RootDir == "" {
		c.RootDir = _defaultRootDir
	}
	return nil
}
