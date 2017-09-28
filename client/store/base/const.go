package base

// DefaultShardIDLength is the number of bytes of file digest to be used for shard ID.
// For every byte, one more level of directories will be created.
const DefaultShardIDLength = 2

// DefaultDirPermission is the default permission for new directories.
const DefaultDirPermission = 0740
