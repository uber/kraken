package blobserver

import (
	"hash"
	"time"

	"code.uber.internal/infra/kraken/lib/hrw"
	"github.com/spaolacci/murmur3"
)

// Config defines the configuration used by Origin cluster for hashing blob digests.
type Config struct {
	NumReplica int                       `yaml:"num_replica"`
	HashNodes  map[string]HashNodeConfig `yaml:"hash_nodes"`
	Repair     RepairConfig              `yaml:"repair"`
}

// HashNodeConfig defines the config for a single origin node
type HashNodeConfig struct {
	Label  string `yaml:"label"`
	Weight int    `yaml:"weight"`
}

// RepairConfig defines the configuration used by Origin cluster for
// running concurrent repairs.
type RepairConfig struct {
	NumWorkers    int           `yaml:"num_workers"`
	MaxRetries    uint          `yaml:"max_retries"`
	RetryDelay    time.Duration `yaml:"retry_delay"`
	MaxRetryDelay time.Duration `yaml:"max_retry_delay"`
	ConnTimeout   time.Duration `yaml:"conn_timeout"`
}

// ClientConfig defines configuration for blobserver HTTP Client.
type ClientConfig struct {
	UploadChunkSize int64 `yaml:"upload_chunk_size"`
}

// LabelToHostname generates a reverse mapping of HashNodes by label to hostname.
func (c Config) LabelToHostname() map[string]string {
	m := make(map[string]string, len(c.HashNodes))
	for host, node := range c.HashNodes {
		m[node.Label] = host
	}
	return m
}

// HashState initializes hash state from the configured hash nodes.
func (c Config) HashState() *hrw.RendezvousHash {
	h := hrw.NewRendezvousHash(
		func() hash.Hash { return murmur3.New64() },
		hrw.UInt64ToFloat64)
	for _, node := range c.HashNodes {
		h.AddNode(node.Label, node.Weight)
	}
	return h
}
