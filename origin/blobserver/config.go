package blobserver

import (
	"errors"
	"hash"
	"sort"
	"time"

	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/utils/dedup"
	"github.com/c2h5oh/datasize"
	"github.com/spaolacci/murmur3"
)

// Config defines the configuration used by Origin cluster for hashing blob digests.
type Config struct {
	NumReplica   int                                     `yaml:"num_replica"`
	HashNodes    HashNodeMap                             `yaml:"hash_nodes"`
	Repair       RepairConfig                            `yaml:"repair"`
	RequestCache dedup.RequestCacheConfig                `yaml:"request_cache"`
	PieceLengths map[datasize.ByteSize]datasize.ByteSize `yaml:"piece_lengths"`
}

// HashNodeMap defines a map from address of HashNodeConfig
// NOTE: Address should be in the form of hostname:port or dns name.
// The reason behind this is that we do not want to make assumption of the port
// each origin is running on.
type HashNodeMap map[string]HashNodeConfig

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

type rangeConfig struct {
	fileSize    int64
	pieceLength int64
}

// pieceLengthConfig represents a sorted list joining file size to torrent piece
// length for all files under said size, for example, these ranges:
//
//   [
//     (0, 1mb),
//     (2gb, 4mb),
//     (4gb, 8mb),
//   ]
//
// are interpreted as:
//
//   N < 2gb           : 1mb
//   N >= 2gb, N < 4gb : 4mb
//   N >= 4gb          : 8mb
//
type pieceLengthConfig struct {
	ranges []rangeConfig
}

func newPieceLengthConfig(
	pieceLengthByFileSize map[datasize.ByteSize]datasize.ByteSize) (*pieceLengthConfig, error) {

	if len(pieceLengthByFileSize) == 0 {
		return nil, errors.New("no piece lengths configured")
	}
	var ranges []rangeConfig
	for fileSize, pieceLength := range pieceLengthByFileSize {
		ranges = append(ranges, rangeConfig{
			fileSize:    int64(fileSize),
			pieceLength: int64(pieceLength),
		})
	}
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].fileSize < ranges[j].fileSize
	})
	return &pieceLengthConfig{ranges}, nil
}

func (c *pieceLengthConfig) get(fileSize int64) int64 {
	pieceLength := c.ranges[0].pieceLength
	for _, r := range c.ranges {
		if fileSize < r.fileSize {
			break
		}
		pieceLength = r.pieceLength
	}
	return pieceLength
}

// LabelToAddress generates a reverse mapping of HashNodes by label to hostname.
func (c Config) LabelToAddress() map[string]string {
	m := make(map[string]string, len(c.HashNodes))
	for addr, node := range c.HashNodes {
		m[node.Label] = addr
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
