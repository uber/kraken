package blobinfo

// Info is empty for now. In the future, it might include things like last access
// time and blob size.
type Info struct{}

// New creates a new Info.
func New() *Info {
	return &Info{}
}
