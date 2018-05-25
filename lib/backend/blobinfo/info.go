package blobinfo

// Info contains metadata about a blob.
type Info struct {
	Size int64
}

// New creates a new Info.
func New(size int64) *Info {
	return &Info{size}
}
