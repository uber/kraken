package blobserver

import (
	"hash"

	"github.com/spaolacci/murmur3"

	"code.uber.internal/infra/kraken/lib/hrw"
)

// RendezvousHashFixture is Rendesvous hash state fixture
func RendezvousHashFixture(config Config) *hrw.RendezvousHash {

	hashState := hrw.NewRendezvousHash(
		func() hash.Hash { return murmur3.New64() },
		hrw.UInt64ToFloat64)

	// Add all configured nodes to a hashing state
	for _, node := range config.HashNodes {
		hashState.AddNode(node.Label, node.Weight)
	}

	return hashState
}
