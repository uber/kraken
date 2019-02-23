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
	"github.com/uber/kraken/utils/syncutil"

	"github.com/willf/bitset"
)

// pieceSelectionPolicy defines a policy for determining which pieces to request
// given a set of candidates and relevant stats about them.
// If 'valid' is not thread-safe, caller must handle locking.
type pieceSelectionPolicy interface {
	selectPieces(
		limit int,
		valid func(int) bool, // whether the given piece is a valid selection or not
		candidates *bitset.BitSet,
		numPeersByPiece syncutil.Counters) ([]int, error)
}
