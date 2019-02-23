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
package storage

import (
	"github.com/uber/kraken/core"

	"github.com/willf/bitset"
)

// TorrentInfo encapsulates read-only torrent information.
type TorrentInfo struct {
	metainfo          *core.MetaInfo
	bitfield          *bitset.BitSet
	percentDownloaded int
}

// NewTorrentInfo creates a new TorrentInfo.
func NewTorrentInfo(mi *core.MetaInfo, bitfield *bitset.BitSet) *TorrentInfo {
	numComplete := bitfield.Count()
	downloaded := int(float64(numComplete) / float64(mi.NumPieces()) * 100)
	return &TorrentInfo{mi, bitfield, downloaded}
}

func (i *TorrentInfo) String() string {
	return i.InfoHash().Hex()
}

// Digest returns the torrent's blob digest.
func (i *TorrentInfo) Digest() core.Digest {
	return i.metainfo.Digest()
}

// InfoHash returns the hash torrent metainfo.
func (i *TorrentInfo) InfoHash() core.InfoHash {
	return i.metainfo.InfoHash()
}

// MaxPieceLength returns the max piece length of the torrent.
func (i *TorrentInfo) MaxPieceLength() int64 {
	return i.metainfo.PieceLength()
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
