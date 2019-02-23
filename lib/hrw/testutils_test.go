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
	"testing"

	"github.com/stretchr/testify/assert"
)

// assertKeyDistribution makes sure the ratio of keys falls in a particular
// bucket conforms to general weight distribution within delta.
func assertKeyDistribution(
	t *testing.T, rh *RendezvousHash, nodekeys NodeKeysTable,
	numKeys int, totalWeights float64, delta float64) {

	for name, v := range nodekeys {
		node, _ := rh.GetNode(name)
		assert.NotEqual(t, node, nil)

		assert.InDelta(
			t, float64(len(v))/float64(numKeys), float64(node.Weight)/totalWeights, delta)
	}
}

type ByScore []float64

func (a ByScore) Len() int           { return len(a) }
func (a ByScore) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByScore) Less(i, j int) bool { return a[i] < a[j] }
