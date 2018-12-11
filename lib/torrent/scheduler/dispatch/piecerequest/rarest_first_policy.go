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
