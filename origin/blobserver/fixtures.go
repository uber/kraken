package blobserver

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"strconv"
	"time"

	"code.uber.internal/infra/kraken/lib/hrw"
	hashcfg "code.uber.internal/infra/kraken/origin/config"
	"code.uber.internal/infra/kraken/utils"
)

// HashConfigFixture returns test configuration for origin's hash
// configuration
func HashConfigFixture(weights []int) hashcfg.HashConfig {
	hashstate := make(map[string]hashcfg.HashNodeConfig)

	for i, w := range weights {
		host := "host_" + strconv.Itoa(i) + ":1234"
		if i == 0 {
			host, _ = utils.GetLocalIP()
		}
		hashstate[host] = hashcfg.HashNodeConfig{
			Label:  "origin" + strconv.Itoa(i),
			Weight: w,
		}
	}

	cf := hashcfg.HashConfig{HashNodes: hashstate, NumReplica: 3,
		Repair: hashcfg.RepairConfig{
			NumWorkers:   10,
			NumRetries:   3,
			RetryDelayMs: time.Duration(200 * time.Millisecond),
		},
	}
	hashcfg.PrepareConfig(&cf)

	return cf
}

// RendezvousHashFixture is Rendesvous hash state fixture
func RendezvousHashFixture(hashConfig hashcfg.HashConfig) *hrw.RendezvousHash {

	hashState := hrw.NewRendezvousHash(
		func() hash.Hash { return murmur3.New64() },
		hrw.UInt64ToFloat64)

	// Add all configured nodes to a hashing state
	for _, node := range hashConfig.HashNodes {
		hashState.AddNode(node.Label, node.Weight)
	}

	return hashState
}
