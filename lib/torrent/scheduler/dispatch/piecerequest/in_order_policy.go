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
	"github.com/uber/kraken/utils/syncutil"

	"github.com/willf/bitset"
)

// InOrderPolicy selects the lowest-index pieces first. It is used to support
// streaming reads, where serving the blob in order benefits from fetching
// earlier pieces sooner.
const InOrderPolicy = "in_order"

type inOrderPolicy struct{}

func newInOrderPolicy() *inOrderPolicy {
	return &inOrderPolicy{}
}

func (p *inOrderPolicy) selectPieces(
	limit int,
	valid func(int) bool,
	candidates *bitset.BitSet,
	numPeersByPiece syncutil.Counters) ([]int, error) {

	pieces := make([]int, 0, limit)
	if limit == 0 {
		return pieces, nil
	}

	for i, e := candidates.NextSet(0); e; i, e = candidates.NextSet(i + 1) {
		if !valid(int(i)) {
			continue
		}
		pieces = append(pieces, int(i))
		if len(pieces) >= limit {
			break
		}
	}

	return pieces, nil
}
