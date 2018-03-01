package core

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"code.uber.internal/infra/kraken/utils/memsize"
)

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
				Info: Info{
					PieceLength: int64(test.pieceLength),
					PieceSums:   pieceSums,
					Name:        "6422b52513a39399598494bdb7471211890cd13c271fb5c11c5ba6538ed7578c",
					Length:      int64(test.blobSize),
				},
				CreationDate: time.Now().Unix(),
				Comment:      "some comment",
				CreatedBy:    "some user",
			}
			b, err := mi.Serialize()
			require.NoError(err)
			size := uint64(len(b))
			require.True(size < redisValueLimit,
				"%d piece serialization %d bytes too large", numPieces, size-redisValueLimit)
		})
	}
}
