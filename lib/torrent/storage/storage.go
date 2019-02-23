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
	"errors"
	"io"

	"github.com/uber/kraken/core"

	"github.com/willf/bitset"
)

// ErrNotFound occurs when TorrentArchive cannot found a torrent.
var ErrNotFound = errors.New("torrent not found")

// ErrPieceComplete occurs when Torrent cannot write a piece because it is already
// complete.
var ErrPieceComplete = errors.New("piece is already complete")

// PieceReader defines operations for lazy piece reading.
type PieceReader interface {
	io.ReadCloser
	Length() int
}

// Torrent represents a read/write interface for a torrent
type Torrent interface {
	Digest() core.Digest
	Stat() *TorrentInfo
	NumPieces() int
	Length() int64
	PieceLength(piece int) int64
	MaxPieceLength() int64
	InfoHash() core.InfoHash
	Complete() bool
	BytesDownloaded() int64
	Bitfield() *bitset.BitSet
	String() string

	HasPiece(piece int) bool
	MissingPieces() []int

	WritePiece(src PieceReader, piece int) error
	GetPieceReader(piece int) (PieceReader, error)
}

// TorrentArchive creates and open torrent file
type TorrentArchive interface {
	Stat(namespace string, d core.Digest) (*TorrentInfo, error)
	CreateTorrent(namespace string, d core.Digest) (Torrent, error)
	GetTorrent(namespace string, d core.Digest) (Torrent, error)
	DeleteTorrent(d core.Digest) error
}
