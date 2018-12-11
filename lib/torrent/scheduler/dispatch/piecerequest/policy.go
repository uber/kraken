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
