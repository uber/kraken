package core

import (
	"bytes"
	"testing"

	"code.uber.internal/infra/kraken/utils/randutil"
	"github.com/stretchr/testify/require"
)

func TestInfoGetPieceLength(t *testing.T) {
	tests := []struct {
		desc        string
		size        uint64
		pieceLength int64
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
			blob := bytes.NewReader(randutil.Text(test.size))
			info, err := NewInfoFromBlob("testblob", blob, test.pieceLength)
			require.NoError(t, err)
			require.Equal(t, test.expected, info.GetPieceLength(test.i))
		})
	}
}
