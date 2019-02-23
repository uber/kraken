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
package piecerequest

import (
	"github.com/uber/kraken/utils/heap"
	"github.com/uber/kraken/utils/syncutil"

	"github.com/willf/bitset"
)

// RarestFirstPolicy selects pieces that the fewest of our peers have to request first.
const RarestFirstPolicy = "rarest_first"

type rarestFirstPolicy struct{}

func newRarestFirstPolicy() *rarestFirstPolicy {
	return &rarestFirstPolicy{}
}

func (p *rarestFirstPolicy) selectPieces(
	limit int,
	valid func(int) bool,
	candidates *bitset.BitSet,
	numPeersByPiece syncutil.Counters) ([]int, error) {

	candidateQueue := heap.NewPriorityQueue()
	for i, e := candidates.NextSet(0); e; i, e = candidates.NextSet(i + 1) {
		candidateQueue.Push(&heap.Item{
			Value:    int(i),
			Priority: numPeersByPiece.Get(int(i)),
		})
	}

	pieces := make([]int, 0, limit)
	for len(pieces) < limit && candidateQueue.Len() > 0 {
		item, err := candidateQueue.Pop()
		if err != nil {
			return nil, err
		}

		candidate := item.Value.(int)
		if valid(candidate) {
			pieces = append(pieces, candidate)
		}
	}

	return pieces, nil
}
