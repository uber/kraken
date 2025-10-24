package proxyserver

import (
	"github.com/c2h5oh/datasize"
	"github.com/uber/kraken/utils/listener"
)

const (
	// DefaultPrefetchMinBlobSize is the default minimum blob size for prefetch (no minimum).
	DefaultPrefetchMinBlobSize = 0
	// DefaultPrefetchMaxBlobSize is the default maximum blob size for prefetch (50GB).
	DefaultPrefetchMaxBlobSize = 50 * datasize.GB
)

type Config struct {
	Listener            listener.Config   `yaml:"listener"`
	PrefetchMinBlobSize datasize.ByteSize `yaml:"prefetch_min_blob_size"` // Minimum size for a blob to be prefetched (e.g., "50M", "1G"). 0 means no minimum.
	PrefetchMaxBlobSize datasize.ByteSize `yaml:"prefetch_max_blob_size"` // Maximum size for a blob to be prefetched (e.g., "10G", "50G"). 0 means no maximum.
}
