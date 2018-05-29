package blobrefresh

import "github.com/c2h5oh/datasize"

// Config defines Refresher configuration.
type Config struct {
	// Limits the size of blobs which origin will accept. A 0 size limit means
	// blob size is unbounded.
	SizeLimit datasize.ByteSize `yaml:"size_limit"`
}
