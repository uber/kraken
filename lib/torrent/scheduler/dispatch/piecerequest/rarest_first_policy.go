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
	"fmt"

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
	valid func(pieceIdx int) bool,
	pieceCandidates *bitset.BitSet,
	numPeersByPiece syncutil.Counters) ([]int, error) {

	candidateQueue := heap.NewPriorityQueue()
	for pieceIdx, ok := pieceCandidates.NextSet(0); ok; pieceIdx, ok = pieceCandidates.NextSet(pieceIdx + 1) {
		candidateQueue.Push(&heap.Item{
			Value:    int(pieceIdx),
			Priority: numPeersByPiece.Get(int(pieceIdx)),
		})
	}

	pieces := make([]int, 0, limit)
	for len(pieces) < limit && candidateQueue.Len() > 0 {
		item, err := candidateQueue.Pop()
		if err != nil {
			return nil, err
		}

		candidate, ok := item.Value.(int)
		if !ok {
			return nil, fmt.Errorf("expected int, got %T", item.Value)
		}
		if valid(candidate) {
			pieces = append(pieces, candidate)
		}
	}

	return pieces, nil
}
