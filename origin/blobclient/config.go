package blobclient

import "code.uber.internal/infra/kraken/utils/memsize"

// Config defines configuration for blobserver HTTP Client.
type Config struct {
	UploadChunkSize int64 `yaml:"upload_chunk_size"`
}

func (c Config) applyDefaults() Config {
	if c.UploadChunkSize == 0 {
		c.UploadChunkSize = int64(50 * memsize.MB)
	}
	return c
}
