package disk

// Config configures [Store].
type Config struct {
	// The capacity of the store in bytes. When breached, LRU eviction is used.
	CapacityBytes uint64
	// The root directory under which [Store]'s blobs and state are stored.
	RootDir string
	// Whether after crash/restart, the Store removes incomplete files from disk (usually to prevent leaks) OR
	// reboots any incomplete files from disk (allowing users to continue the blob download/upload, where it was left off before the crash).
	RebootIncompleteBlobs bool
	// If > 0, directory sharding is used to speed up performance, where ShardLength denotes
	// 1) the length of each directory shard's name and 2) the number of shards.
	// A value of 0 denotes no sharding.
	ShardLength int // TODO - add a mechanism to migrate in-place from 1 shardLength to another.
}
