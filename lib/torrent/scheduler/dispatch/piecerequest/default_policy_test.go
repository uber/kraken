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

func TestDefaultPolicySelectsPiecesUniformly(t *testing.T) {
	tests := []struct {
		desc      string
		numPieces int
		limit     int
	}{
		{"one of two", 2, 1},
		{"one of four", 4, 1},
		{"two of six", 6, 2},
		{"three of ten", 10, 3},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			const trials = 20000

			all := make([]bool, test.numPieces)
			for i := range all {
				all[i] = true
			}
			candidates := bitsetutil.FromBools(all...)
			valid := func(int) bool { return true }

			p := newDefaultPolicy()
			counts := make([]int, test.numPieces)
			for i := 0; i < trials; i++ {
				pieces, err := p.selectPieces(
					test.limit, valid, candidates, syncutil.NewCounters(test.numPieces))
				require.NoError(err)
				require.Len(pieces, test.limit)
				for _, piece := range pieces {
					counts[piece]++
				}
			}

			// Every piece should be selected in limit/numPieces of the trials.
			// The allowed deviation is at least 8 standard deviations.
			expected := float64(trials * test.limit / test.numPieces)
			for piece, count := range counts {
				require.InDelta(expected, count, 0.1*expected, "piece %d", piece)
			}
		})
	}
}
