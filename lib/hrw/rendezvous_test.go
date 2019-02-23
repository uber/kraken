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
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"reflect"
	"runtime"
	"sort"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
)

func TestScoreFunctionFloatPrecision(t *testing.T) {
	t.Parallel()

	byteLength := []int{8, 16, 32} // 64, 128, 256 bits

	for index, bl := range byteLength {
		for indexScore, scoreFunc := range []UIntToFloat{BigIntToFloat64, UInt64ToFloat64} {
			// UInt64ToFloat64 can't work on values > 64 bits
			if index > 0 && indexScore > 0 {
				continue
			}
			maxHashValue := make([]byte, bl)
			val := make([]byte, bl)

			for i := 0; i < bl; i++ {
				maxHashValue[i] = 0xFF
				val[i] = 0
			}

			val[len(val)-1] = 1
			floatVal := scoreFunc(val, maxHashValue, nil)
			assert.NotEqual(t, floatVal, 0.0)
			assert.Equal(t, math.IsNaN(math.Log(floatVal)), false)
			assert.Equal(t, math.IsInf(math.Log(floatVal), 1), false)
			assert.Equal(t, math.IsInf(math.Log(floatVal), -1), false)
		}
	}
}

func TestScoreFunctionUint64ToFloat64BadValues(t *testing.T) {
	t.Parallel()

	maxHashValue := make([]byte, 8)
	for i := 0; i < 8; i++ {
		maxHashValue[i] = 0xFF
	}

	u64val := (1 << 53)
	for i := 0; i <= 11; i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(u64val))

		floatVal := UInt64ToFloat64(b, maxHashValue, nil)

		assert.Equal(t, floatVal, 0.0)

		floatVal = UInt64ToFloat64(b, maxHashValue, murmur3.New64())

		assert.NotEqual(t, floatVal, 0.0)
		assert.Equal(t, math.IsNaN(math.Log(floatVal)), false)
		assert.Equal(t, math.IsInf(math.Log(floatVal), 1), false)
		assert.Equal(t, math.IsInf(math.Log(floatVal), -1), false)
		u64val = u64val << 1

	}
	u64val = (1 << 53)
	for i := 0; i <= 11; i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(u64val))

		floatVal := UInt64ToFloat64(b, maxHashValue, murmur3.New64())

		assert.NotEqual(t, floatVal, 0.0)
		assert.Equal(t, math.IsNaN(math.Log(floatVal)), false)
		assert.Equal(t, math.IsInf(math.Log(floatVal), 1), false)
		assert.Equal(t, math.IsInf(math.Log(floatVal), -1), false)
		u64val = u64val << 1
	}
}

func TestKeyDistributionAndNodeChanges(t *testing.T) {
	t.Parallel()

	hashes := []struct {
		name string
		f    HashFactory
	}{
		{"murmur3", Murmur3Hash},
		{"sha256", sha256.New},
		{"md5", md5.New},
	}

	scoreFuncs := []struct {
		name string
		f    UIntToFloat
	}{
		{"BigIntToFloat64", BigIntToFloat64},
		{"UInt64ToFloat64", UInt64ToFloat64},
	}
	numKeys := 1000

	tests := []func(int, HashFactory, UIntToFloat, *testing.T){
		testKeyDistribution,
		testAddNodes,
		testRemoveNodes,
		testReturnNodesLength,
		testReturnNodesOrder,
		testAddingCapacity,
		testRemovingCapacity,
	}

	for _, hash := range hashes {
		for _, scoreFunc := range scoreFuncs {
			t.Run(hash.name+scoreFunc.name, func(t *testing.T) {
				for _, test := range tests {
					testName := runtime.FuncForPC(reflect.ValueOf(test).Pointer()).Name()
					t.Run(testName, func(*testing.T) {
						test(numKeys, hash.f, scoreFunc.f, t)
					})
				}
			})
		}
	}
}

func testKeyDistribution(numKeys int, hash HashFactory, scoreFunc UIntToFloat, t *testing.T) {
	rh, nodekeys := RendezvousHashFixture(numKeys, hash, scoreFunc, 100, 200, 400, 800)
	assertKeyDistribution(t, rh, nodekeys, numKeys, 1500.0, 0.1)
}

func testAddNodes(numKeys int, hash HashFactory, scoreFunc UIntToFloat, t *testing.T) {
	rh, nodekeys := RendezvousHashFixture(numKeys, hash, scoreFunc, 100, 200, 400, 800)

	rh.RemoveNode("1")
	assert.Equal(t, len(rh.Nodes), 3)

	for name, v := range nodekeys {
		if name == "1" {
			// "1" node is going to be relocated to other nodes.
			continue
		}
		// The rmaining nodes should not change their allocation buckets.
		for key := range v {
			nodes := rh.GetOrderedNodes(key, 1)
			assert.Equal(t, nodes[0].Label, name)
		}
	}
}

func testRemoveNodes(numKeys int, hash HashFactory, scoreFunc UIntToFloat, t *testing.T) {
	rh, nodekeys := RendezvousHashFixture(numKeys, hash, scoreFunc, 100, 200, 400, 800)

	rh.AddNode("4", 200)
	nodekeys["4"] = make(map[string]struct{})

	assert.Equal(t, len(rh.Nodes), 5)

	for name, v := range nodekeys {
		if name == "4" {
			// New node "4" will get some keys from other nodes.
			continue
		}
		// Th remaining nodes should not change their allocation buckets.
		for key := range v {
			nodes := rh.GetOrderedNodes(key, 1)
			if nodes[0].Label != name {
				assert.Equal(t, nodes[0].Label, "4")
				nodekeys[nodes[0].Label][key] = struct{}{}
				delete(nodekeys[name], key)
			}
		}
	}
	assertKeyDistribution(t, rh, nodekeys, numKeys, 1700.0, 0.1)
}

func testReturnNodesLength(numKeys int, hash HashFactory, scoreFunc UIntToFloat, t *testing.T) {
	rh, _ := RendezvousHashFixture(0, hash, scoreFunc, 100, 200, 400, 800)
	keys := HashKeyFixture(1, hash)

	var scores []float64
	for _, node := range rh.Nodes {
		score := node.Score(keys[0])
		scores = append(scores, score)
	}
	sort.Sort(ByScore(scores))
	nodes := rh.GetOrderedNodes(keys[0], 4)
	assert.Equal(t, len(nodes), 4)
}

func testReturnNodesOrder(numKeys int, hash HashFactory, scoreFunc UIntToFloat, t *testing.T) {
	rh, _ := RendezvousHashFixture(0, hash, scoreFunc, 100, 200, 400, 800)
	keys := HashKeyFixture(1, hash)

	var scores []float64
	for _, node := range rh.Nodes {
		score := node.Score(keys[0])
		scores = append(scores, score)
	}
	sort.Sort(ByScore(scores))
	nodes := rh.GetOrderedNodes(keys[0], 4)
	for index, node := range nodes {
		score := node.Score(keys[0])
		assert.Equal(t, score, scores[4-index-1])
	}
}

func testAddingCapacity(numKeys int, hash HashFactory, scoreFunc UIntToFloat, t *testing.T) {
	rh, nodekeys := RendezvousHashFixture(numKeys, hash, scoreFunc, 100, 200, 400, 800)

	_, index := rh.GetNode("3")
	rh.Nodes[index].Weight = 1000

	for name, v := range nodekeys {
		// Some keys in nodes should change their allocation buckets
		// accomdate for new capacity on node "3".
		for key := range v {
			nodes := rh.GetOrderedNodes(key, 1)
			if nodes[0].Label != name {
				assert.Equal(t, nodes[0].Label, "3")
				nodekeys[nodes[0].Label][key] = struct{}{}
				delete(nodekeys[name], key)
			} else {
				assert.Equal(t, nodes[0].Label, name)
			}
		}
	}

	// Make sure we still keep the target distribution after resharding.
	assertKeyDistribution(t, rh, nodekeys, numKeys, 1700.0, 0.1)
}

func testRemovingCapacity(numKeys int, hash HashFactory, scoreFunc UIntToFloat, t *testing.T) {
	rh, nodekeys := RendezvousHashFixture(numKeys, hash, scoreFunc, 100, 200, 400, 800)

	_, index := rh.GetNode("3")
	rh.Nodes[index].Weight = 200

	for name, v := range nodekeys {
		// The remaining nodes should not change their allocation buckets.
		for key := range v {
			nodes := rh.GetOrderedNodes(key, 1)
			if nodes[0].Label != name {
				assert.Equal(t, name, "3")
				assert.NotEqual(t, nodes[0].Label, "3")
				nodekeys[nodes[0].Label][key] = struct{}{}
				delete(nodekeys[name], key)
			} else {
				assert.Equal(t, nodes[0].Label, name)
			}
		}
	}

	// Make sure we still keep the target distribution after resharding.
	assertKeyDistribution(t, rh, nodekeys, numKeys, 900.0, 0.1)
}
