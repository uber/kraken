package client

import (
	"time"

	"code.uber.internal/infra/kraken/utils/memsize"
)

const (
	defaultChunkSize = 50 * memsize.MB
	defaultTimeout   = 60 * time.Second
)

// Config specifies configuration for BlobAPIClient
type Config struct {
	RequestTimeout time.Duration
	OriginAddr     string
	TrackerAddr    string
	ChunkSize      int64
}
