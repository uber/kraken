package storage

import (
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/bitsetutil"
	"code.uber.internal/infra/kraken/utils/randutil"
)

// TorrentInfoFixture returns a randomly generated TorrentInfo for testing purposes.
func TorrentInfoFixture(size, pieceLength uint64) *TorrentInfo {
	mi := core.SizedBlobFixture(size, pieceLength).MetaInfo
	bitfield := bitsetutil.FromBools(randutil.Bools(mi.Info.NumPieces())...)
	return NewTorrentInfo(mi, bitfield)
}
