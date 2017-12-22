package storage

import "code.uber.internal/infra/kraken/torlib"

// TorrentInfo encapsulates read-only torrent information.
type TorrentInfo struct {
	metainfo          *torlib.MetaInfo
	bitfield          Bitfield
	percentDownloaded int
}

func newTorrentInfo(mi *torlib.MetaInfo, bitfield Bitfield) *TorrentInfo {
	var numComplete int
	for _, b := range bitfield {
		if b {
			numComplete++
		}
	}
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
func (i *TorrentInfo) Bitfield() Bitfield {
	c := make([]bool, len(i.bitfield))
	copy(c, i.bitfield)
	return c
}
