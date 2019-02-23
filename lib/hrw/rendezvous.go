// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package hrw

import (
	"encoding/binary"
	"encoding/hex"
	"hash"
	"math"
	"math/big"
	"sort"

	"github.com/spaolacci/murmur3"
)

// min between two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// HashFactory is a function object for Hash.New() constructor.
type HashFactory func() hash.Hash

// Murmur3Hash is a murmur3 HashFactory.
func Murmur3Hash() hash.Hash { return murmur3.New64() }

// UIntToFloat is a conversion function from uint64 to float64.
// Int could be potentially very big integer, like 256 bits long.
type UIntToFloat func(bytesUInt []byte, maxValue []byte, hasher hash.Hash) float64

// RendezvousHashNode represents a weighted node in a hashing schema.
type RendezvousHashNode struct {
	RHash  *RendezvousHash // parent hash structure with all the configuration
	Label  string          // some string ientifying a unique node label
	Weight int             // node weight, usually denotes node's capacity
}

// RendezvousHash represents a Rendezvous Hashing schema.
// It does not make any assumption about concurrency model so synchronizing
// access to it is a caller's responsibility.
type RendezvousHash struct {
	Hash         HashFactory           // hash function
	ScoreFunc    UIntToFloat           // conversion function from generated hash to float64
	Nodes        []*RendezvousHashNode // all nodes
	MaxHashValue []byte
}

// RendezvousNodesByScore is a predicat that supports sorting by score(key).
type RendezvousNodesByScore struct {
	key   string
	nodes []*RendezvousHashNode
}

// Len return length.
func (a RendezvousNodesByScore) Len() int { return len(a.nodes) }

// Swap swaps two elements.
func (a RendezvousNodesByScore) Swap(i, j int) { a.nodes[i], a.nodes[j] = a.nodes[j], a.nodes[i] }

// Less is a predicate '<' for a set.
func (a RendezvousNodesByScore) Less(i, j int) bool {
	return a.nodes[i].Score(a.key) < a.nodes[j].Score(a.key)
}

// NewRendezvousHash constructs and prepopulates a RendezvousHash object.
func NewRendezvousHash(hashFactory HashFactory, scoreFunc UIntToFloat) *RendezvousHash {
	rh := &RendezvousHash{
		Hash:      hashFactory,
		ScoreFunc: scoreFunc,
	}
	hashLen := len(hashFactory().Sum(nil))

	rh.MaxHashValue = make([]byte, hashLen)
	for i := 0; i < hashLen; i++ {
		rh.MaxHashValue[i] = 0xFF
	}
	return rh
}

// UInt64ToFloat64 Converts a uniformly random 64-bit integer
// to "uniformly" random floating point number on interval [0, 1)
// The approach is heavily based on this material
// https://crypto.stackexchange.com/questions/31657/uniformly-distributed-secure-floating-point-numbers-in-0-1
// and this https://en.wikipedia.org/wiki/Rendezvous_hashing
func UInt64ToFloat64(bytesUInt []byte, maxValue []byte, hasher hash.Hash) float64 {
	maxUInt := binary.BigEndian.Uint64(maxValue)
	fiftyThreeOnes := uint64(maxUInt >> (64 - 53))
	fiftyThreeZeros := float64(1 << 53)
	u64val := binary.BigEndian.Uint64(bytesUInt)

	// val & 0xFFF000000000000 == 0 need to be handled differently
	// as it will result in zeros: something that score
	// function cannot survive. So there are 2^11 keys like that
	// need to be re-hashed one more time. That will introduce a tiny bias
	// in hashing key space distribution that we can live with
	val := u64val & fiftyThreeOnes
	if val == 0 && hasher != nil {
		hasher.Reset()
		hasher.Write(bytesUInt)

		val = binary.BigEndian.Uint64(hasher.Sum(nil)) & fiftyThreeOnes
	}
	return float64(val) / fiftyThreeZeros
}

// BigIntToFloat64 converts BigInt to float64.
func BigIntToFloat64(bytesUInt []byte, maxValue []byte, hasher hash.Hash) float64 {
	maxHashFloat := new(big.Float)
	maxHashFloat.SetInt(new(big.Int).SetBytes(maxValue))

	hashInt := new(big.Int)
	// checksumHash is being consumed as a big endian int.
	hashInt.SetBytes(bytesUInt)

	hashFloat := new(big.Float).SetInt(hashInt)

	// float64's precision, we would not need more then that
	// as we eventually cast everything to float64
	// Big Float will use greater presicions in operations
	hashFloat.SetPrec(53)

	fl64value, _ := hashFloat.Quo(hashFloat, maxHashFloat).Float64()

	// I don't expact that to happen, the accuracy of 256 bits division
	// arithmetic is well within float'64 theoretical minimum for a single
	// division and we always divide with a non zero constant.
	if hashFloat.IsInf() {
		panic("Float64.Quo operation has failed")
	}

	return fl64value
}

// Score computes score of a key for this node in accordance to Weighted
// Rendezvous Hash. It's using big golang float key as hexidemical encoding of
// a byte array.
func (rhn *RendezvousHashNode) Score(key string) float64 {
	hasher := rhn.RHash.Hash()

	keyBytes, err := hex.DecodeString(key)
	if err != nil {
		return math.NaN()
	}

	hashBytes := make([]byte, len(keyBytes)+len(rhn.Label))
	// Add node's seed to a key string
	hashBytes = append(keyBytes, []byte(rhn.Label)...)

	hasher.Write(hashBytes)
	score := rhn.RHash.ScoreFunc(hasher.Sum(nil), rhn.RHash.MaxHashValue, hasher)

	// for more information on this math please look at this paper:
	// http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.414.9353&rep=rep1&type=pdf
	// and this presentation slides:
	// http://www.snia.org/sites/default/files/SDC15_presentations/dist_sys/Jason_Resch_New_Consistent_Hashings_Rev.pdf

	return -float64(rhn.Weight) / math.Log(score)
}

// AddNode adds a node to a hashing ring.
func (rh *RendezvousHash) AddNode(seed string, weight int) {
	node := &RendezvousHashNode{
		RHash:  rh,
		Label:  seed,
		Weight: weight,
	}
	rh.Nodes = append(rh.Nodes, node)
}

// RemoveNode removes a node from a hashing ring.
func (rh *RendezvousHash) RemoveNode(name string) {
	for i, node := range rh.Nodes {
		if node.Label == name {
			rh.Nodes = append(rh.Nodes[:i], rh.Nodes[i+1:]...)
			break
		}
	}
}

// GetNode gets a node from a hashing ring and its index in array.
func (rh *RendezvousHash) GetNode(name string) (*RendezvousHashNode, int) {
	for index, node := range rh.Nodes {
		if node.Label == name {
			return node, index
		}
	}
	return nil, -1
}

// GetOrderedNodes gets an ordered set of N nodes for a key where
// score(Node1) > score(N2) > ... score(NodeN).
// Number of returned nodes = min(N, len(nodes)).
func (rh *RendezvousHash) GetOrderedNodes(key string, n int) []*RendezvousHashNode {
	nodes := make([]*RendezvousHashNode, len(rh.Nodes))
	copy(nodes, rh.Nodes)

	sort.Sort(sort.Reverse(&RendezvousNodesByScore{key: key, nodes: nodes}))

	if n >= len(nodes) {
		return nodes
	}
	return nodes[:n]
}
