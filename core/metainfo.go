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
package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/jackpal/bencode-go"
)

// info contains the "instructions" for how to download / seed a torrent,
// primarily describing how a blob is broken up into pieces and how to verify
// those pieces (i.e. the piece sums).
type info struct {
	// Exported for bencoding.
	PieceLength int64
	PieceSums   []uint32
	Name        string
	Length      int64
}

// Hash computes the InfoHash of info.
func (info *info) Hash() (InfoHash, error) {
	var b bytes.Buffer
	if err := bencode.Marshal(&b, *info); err != nil {
		return InfoHash{}, fmt.Errorf("bencode: %s", err)
	}
	return NewInfoHashFromBytes(b.Bytes()), nil
}

// MetaInfo contains torrent metadata.
type MetaInfo struct {
	info     info
	infoHash InfoHash
	digest   Digest
}

// NewMetaInfo creates a new MetaInfo. Assumes that d is the valid digest for
// blob (re-computing it is expensive).
func NewMetaInfo(d Digest, blob io.Reader, pieceLength int64) (*MetaInfo, error) {
	length, pieceSums, err := calcPieceSums(blob, pieceLength)
	if err != nil {
		return nil, err
	}
	info := info{
		PieceLength: pieceLength,
		PieceSums:   pieceSums,
		Name:        d.Hex(),
		Length:      length,
	}
	h, err := info.Hash()
	if err != nil {
		return nil, fmt.Errorf("compute info hash: %s", err)
	}
	return &MetaInfo{
		info:     info,
		infoHash: h,
		digest:   d,
	}, nil
}

// InfoHash returns the torrent InfoHash.
func (mi *MetaInfo) InfoHash() InfoHash {
	return mi.infoHash
}

// Digest returns the digest of the original blob.
func (mi *MetaInfo) Digest() Digest {
	return mi.digest
}

// Length returns the length of the original blob.
func (mi *MetaInfo) Length() int64 {
	return mi.info.Length
}

// NumPieces returns the number of pieces in the torrent.
func (mi *MetaInfo) NumPieces() int {
	return len(mi.info.PieceSums)
}

// PieceLength returns the piece length used to break up the original blob. Note,
// the final piece may be shorter than this. Use GetPieceLength for the true
// lengths of each piece.
func (mi *MetaInfo) PieceLength() int64 {
	return mi.info.PieceLength
}

// GetPieceLength returns the length of piece i.
func (mi *MetaInfo) GetPieceLength(i int) int64 {
	if i < 0 || i >= len(mi.info.PieceSums) {
		return 0
	}
	if i == len(mi.info.PieceSums)-1 {
		// Last piece.
		return mi.info.Length - mi.info.PieceLength*int64(i)
	}
	return mi.info.PieceLength
}

// GetPieceSum returns the checksum of piece i. Does not check bounds.
func (mi *MetaInfo) GetPieceSum(i int) uint32 {
	return mi.info.PieceSums[i]
}

// metaInfoJSON is used for serializing / deserializing MetaInfo.
type metaInfoJSON struct {
	// Only serialize info for backwards compatibility.
	Info info `json:"Info"`
}

// Serialize converts mi to a json blob.
func (mi *MetaInfo) Serialize() ([]byte, error) {
	return json.Marshal(&metaInfoJSON{mi.info})
}

// DeserializeMetaInfo reconstructs a MetaInfo from a json blob.
func DeserializeMetaInfo(data []byte) (*MetaInfo, error) {
	var j metaInfoJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("json: %s", err)
	}
	h, err := j.Info.Hash()
	if err != nil {
		return nil, fmt.Errorf("compute info hash: %s", err)
	}
	d, err := NewSHA256DigestFromHex(j.Info.Name)
	if err != nil {
		return nil, fmt.Errorf("parse name: %s", err)
	}
	return &MetaInfo{
		info:     j.Info,
		infoHash: h,
		digest:   d,
	}, nil
}

// calcPieceSums hashes blob content in pieceLength chunks.
func calcPieceSums(blob io.Reader, pieceLength int64) (length int64, pieceSums []uint32, err error) {
	if pieceLength <= 0 {
		return 0, nil, errors.New("piece length must be positive")
	}
	for {
		h := PieceHash()
		n, err := io.CopyN(h, blob, pieceLength)
		if err != nil && err != io.EOF {
			return 0, nil, fmt.Errorf("read blob: %s", err)
		}
		length += n
		if n == 0 {
			break
		}
		sum := h.Sum32()
		pieceSums = append(pieceSums, sum)
		if n < pieceLength {
			break
		}
	}
	return length, pieceSums, nil
}
