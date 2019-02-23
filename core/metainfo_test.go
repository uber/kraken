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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/utils/memsize"
)

func TestMetaInfoGetPieceLength(t *testing.T) {
	tests := []struct {
		desc        string
		size        uint64
		pieceLength uint64
		i           int
		expected    int64
	}{
		{"first piece", 10, 3, 0, 3},
		{"smaller last piece", 10, 3, 3, 1},
		{"same size last piece", 8, 2, 3, 2},
		{"middle piece", 10, 3, 1, 3},
		{"outside bounds", 10, 3, 4, 0},
		{"negative", 10, 3, -1, 0},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			blob := SizedBlobFixture(test.size, test.pieceLength)
			require.Equal(t, test.expected, blob.MetaInfo.GetPieceLength(test.i))
		})
	}
}

func TestMetaInfoSerialization(t *testing.T) {
	require := require.New(t)

	blob := NewBlobFixture()

	b, err := blob.MetaInfo.Serialize()
	require.NoError(err)
	result, err := DeserializeMetaInfo(b)
	require.NoError(err)
	require.Equal(blob.Digest, result.Digest())
	require.Equal(blob.MetaInfo.InfoHash(), result.InfoHash())
}

func TestMetaInfoBackwardsCompatibility(t *testing.T) {
	require := require.New(t)

	// This metainfo / hash pair was taken from a production agent. It should
	// be deserializable by the new logic and produce the same info hash.
	// TODO(codyg): This test can be removed once this change is fully rolled
	// out.
	rawMetaInfo := `{"Info":{"PieceLength":4194304,"PieceSums":[2131691452],"Name":"289314c356bc2a19802c3e31505506db30ea81a0bcaea4ec3e079524c8ac3cf5","Length":236},"Announce":"","AnnounceList":null,"CreationDate":0,"Comment":"","CreatedBy":""}`

	expectedInfoHash, err := NewInfoHashFromHex("85b978c4377625b3963df406d0dd3a1da5a7d9c3")
	require.NoError(err)

	result, err := DeserializeMetaInfo([]byte(rawMetaInfo))
	require.NoError(err)
	require.Equal(expectedInfoHash, result.InfoHash())
}

func TestMetaInfoSerializationLimit(t *testing.T) {

	// MetaInfo is stored as raw bytes as a Redis value, and should stay
	// within the limits of the value. Because the number of pieces in a
	// torrent is variable, this test serves as a sanity check that reasonable
	// blob size / piece length combinations can be safely serialized.
	var redisValueLimit = 512 * memsize.MB

	tests := []struct {
		description string
		blobSize    uint64
		pieceLength uint64
	}{
		{"reasonable default", 2 * memsize.GB, 2 * memsize.MB},
		{"large file", 100 * memsize.GB, 2 * memsize.MB},
		{"tiny pieces", 2 * memsize.GB, 4 * memsize.KB},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			require := require.New(t)

			numPieces := test.blobSize / test.pieceLength
			pieceSums := make([]uint32, numPieces)
			for i := range pieceSums {
				pieceSums[i] = rand.Uint32()
			}

			mi := MetaInfo{
				info: info{
					PieceLength: int64(test.pieceLength),
					PieceSums:   pieceSums,
					Name:        "6422b52513a39399598494bdb7471211890cd13c271fb5c11c5ba6538ed7578c",
					Length:      int64(test.blobSize),
				},
			}
			b, err := mi.Serialize()
			require.NoError(err)
			size := uint64(len(b))
			require.True(size < redisValueLimit,
				"%d piece serialization %d bytes too large", numPieces, size-redisValueLimit)
		})
	}
}
