package base

// DefaultShardIDLength is the number of bytes of file digest to be used for shard ID.
// For every byte (2 HEX char), one more level of directories will be created.
const DefaultShardIDLength = 2

// DefaultDirPermission is the default permission for new directories.
const DefaultDirPermission = 0775

// DefaultDataFileName is the name of the actual blob data file.
const DefaultDataFileName = "data"
