package storage

import (
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/bitsetutil"
	"github.com/uber/kraken/utils/randutil"
)

// TorrentInfoFixture returns a randomly generated TorrentInfo for testing purposes.
func TorrentInfoFixture(size, pieceLength uint64) *TorrentInfo {
	mi := core.SizedBlobFixture(size, pieceLength).MetaInfo
	bitfield := bitsetutil.FromBools(randutil.Bools(mi.NumPieces())...)
	return NewTorrentInfo(mi, bitfield)
}
