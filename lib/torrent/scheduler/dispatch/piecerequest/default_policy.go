package piecerequest

import (
	"math/rand"

	"code.uber.internal/infra/kraken/utils/syncutil"

	"github.com/willf/bitset"
)

// DefaultPolicy randomly selects pieces to request.
const DefaultPolicy = "default"

type defaultPolicy struct{}

func newDefaultPolicy() *defaultPolicy {
	return &defaultPolicy{}
}

func (p *defaultPolicy) selectPieces(
	limit int,
	valid func(int) bool,
	candidates *bitset.BitSet,
	numPeersByPiece syncutil.Counters) ([]int, error) {

	pieces := make([]int, 0, limit)
	if limit == 0 {
		return pieces, nil
	}

	// Reservoir sampling.
	var k int
	for i, e := candidates.NextSet(0); e; i, e = candidates.NextSet(i + 1) {
		if !valid(int(i)) {
			continue
		}

		// Fill the 'reservoir' until full.
		if len(pieces) < limit {
			pieces = append(pieces, int(i))

			// Replace elements in the 'reservoir' with decreasing probability.
		} else {
			j := rand.Intn(k)
			if j < limit {
				pieces[j] = int(i)
			}
		}
		k++
	}

	return pieces, nil
}
