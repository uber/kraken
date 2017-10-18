package blobserver

import (
	"fmt"
	"hash"
	"strings"
	"time"

	"code.uber.internal/infra/kraken/lib/hrw"
	"github.com/spaolacci/murmur3"
)

// Config defines the configuration used by Origin cluster for hashing blob digests.
type Config struct {
	NumReplica int          `yaml:"num_replica"`
	HashNodes  HashNodeMap  `yaml:"hash_nodes"`
	Repair     RepairConfig `yaml:"repair"`
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

// ClientConfig defines configuration for blobserver HTTP Client.
type ClientConfig struct {
	UploadChunkSize int64 `yaml:"upload_chunk_size"`
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

type portLookupError struct {
	hostname string
	numPorts int
}

func (e portLookupError) Error() string {
	return fmt.Sprintf(
		"invalid hashnode configuration: %s should have exactly one port, actual number of ports: %d",
		e.hostname, e.numPorts)
}

type parseAddrError struct {
	addr string
}

func (e parseAddrError) Error() string {
	return fmt.Sprintf("cannot parse node addr %q: expected format host:port or host", e.addr)
}

// GetAddr searches hash node config for hostname and returns the full address.
func (c Config) GetAddr(hostname string) (string, error) {
	var ports []string
	for addr := range c.HashNodes {
		var h, p string
		parts := strings.Split(addr, ":")
		if len(parts) == 2 {
			h = parts[0]
			p = parts[1]
			if p == "" {
				return "", parseAddrError{addr}
			}
		} else if len(parts) == 1 {
			h = parts[0]
		} else {
			return "", parseAddrError{addr}
		}
		if h == hostname {
			ports = append(ports, p)
		}
	}
	if len(ports) != 1 {
		return "", portLookupError{hostname, len(ports)}
	}
	addr := hostname
	if ports[0] != "" {
		addr += ":" + ports[0]
	}
	return addr, nil
}
