package storage

import (
	"code.uber.internal/infra/kraken/torlib"

	"github.com/willf/bitset"
)

// TorrentInfo encapsulates read-only torrent information.
type TorrentInfo struct {
	metainfo          *torlib.MetaInfo
	bitfield          *bitset.BitSet
	percentDownloaded int
}

func newTorrentInfo(mi *torlib.MetaInfo, bitfield *bitset.BitSet) *TorrentInfo {
	numComplete := bitfield.Count()
	downloaded := int(float64(numComplete) / float64(len(mi.Info.PieceSums)) * 100)
	return &TorrentInfo{mi, bitfield, downloaded}
}

func (i *TorrentInfo) String() string {
	return i.InfoHash().HexString()
}

// Name returns the torrent file name.
func (i *TorrentInfo) Name() string {
	return i.metainfo.Name()
}

// InfoHash returns the hash torrent metainfo.
func (i *TorrentInfo) InfoHash() torlib.InfoHash {
	return i.metainfo.InfoHash
}

// MaxPieceLength returns the max piece length of the torrent.
func (i *TorrentInfo) MaxPieceLength() int64 {
	return i.metainfo.Info.PieceLength
}

// PercentDownloaded returns the percent of bytes downloaded as an integer
// between 0 and 100. Useful for logging.
func (i *TorrentInfo) PercentDownloaded() int {
	return i.percentDownloaded
}

// Bitfield returns the piece status bitfield of the torrent. Note, this is a
// snapshot and may be stale information.
func (i *TorrentInfo) Bitfield() *bitset.BitSet {
	return i.bitfield
}
