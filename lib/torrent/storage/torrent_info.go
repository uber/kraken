package storage

import "code.uber.internal/infra/kraken/torlib"

// TorrentInfo encapsulates read-only torrent information.
type TorrentInfo struct {
	metainfo *torlib.MetaInfo
	bitfield Bitfield
}

func newTorrentInfo(mi *torlib.MetaInfo, b Bitfield) *TorrentInfo {
	return &TorrentInfo{mi, b}
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

// Bitfield returns the piece status bitfield of the torrent. Note, this is a
// snapshot and may be stale information.
func (i *TorrentInfo) Bitfield() Bitfield {
	c := make([]bool, len(i.bitfield))
	copy(c, i.bitfield)
	return c
}
