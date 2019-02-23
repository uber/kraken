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
	"crypto/rand"
	"encoding/hex"
	"strconv"
)

// NodeKeysTable is a Node to keys map utility type
type NodeKeysTable map[string]map[string]struct{}

// All functions defined in this file are intended for testing purposes only.

// RendezvousHashFixture creates a weigthed rendezvous hashing object with
// specified #numKey of keys, a hash function (we use sha256.New primarely for testing) and
// the list of weights which define nodes in rendezvous hashing schema and assign weights to
// them in their natural order. The nodes will be also initialized with seed names equal
// to their corresponding indices in the array, so for
// RendezvousHashFixture(10, sha256.New, 100, 200, 300) there will be a RendezvousHash object created
// with 3 nodes "0": 100, "1":200, "2":300
// The fixture will return RendezvousHash object and the node key buckets table
func RendezvousHashFixture(numKeys int, hashFactory HashFactory, scoreFunc UIntToFloat, weights ...int) (*RendezvousHash, map[string]map[string]struct{}) {
	rh := NewRendezvousHash(hashFactory, scoreFunc)

	keys := NodeKeysTable{}
	b := make([]byte, 64)

	totalWeights := 0
	for index, weight := range weights {
		totalWeights += weight
		rh.AddNode(strconv.Itoa(index), weight)
		keys[strconv.Itoa(index)] = make(map[string]struct{})
	}
	// 1500 the sum of all weights
	for i := 0; i < numKeys; i++ {
		rand.Read(b)
		key := hex.EncodeToString(b)
		nodes := rh.GetOrderedNodes(key, 1)
		keys[nodes[0].Label][key] = struct{}{}
	}

	return rh, keys
}

//HashKeyFixture generate #numkeys random keys according to a hash function
func HashKeyFixture(numKeys int, hashFactory HashFactory) []string {
	var keys []string
	b := make([]byte, 64)

	for i := 0; i < numKeys; i++ {
		rand.Read(b)
		key := hex.EncodeToString(b)
		keys = append(keys, key)
	}

	return keys
}
