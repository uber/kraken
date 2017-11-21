package torlib

import (
	"hash"
	"hash/crc32"
)

// PieceHash returns the hash used to sum pieces.
func PieceHash() hash.Hash32 {
	return crc32.NewIEEE()
}
