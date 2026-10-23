// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package piecerequest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/utils/bitsetutil"
	"github.com/uber/kraken/utils/syncutil"
)

func TestDefaultPolicySamplesPiecesUniformly(t *testing.T) {
	const attempts = 10000
	counts := make([]int, 3)
	policy := newDefaultPolicy()
	candidates := bitsetutil.FromBools(true, true, true)
	peers := syncutil.NewCounters(len(counts))

	for range attempts {
		pieces, err := policy.selectPieces(1, func(int) bool { return true }, candidates, peers)
		require.NoError(t, err)
		require.Len(t, pieces, 1)
		counts[pieces[0]]++
	}

	expected := float64(attempts) / float64(len(counts))
	for piece, count := range counts {
		require.InDelta(t, expected, count, expected*0.1, "piece %d selected %d times", piece, count)
	}
}
