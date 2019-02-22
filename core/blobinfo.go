package core

// BlobInfo contains metadata about a blob.
type BlobInfo struct {
	Size int64
}

// NewBlobInfo creates a new BlobInfo.
func NewBlobInfo(size int64) *BlobInfo {
	return &BlobInfo{size}
}
