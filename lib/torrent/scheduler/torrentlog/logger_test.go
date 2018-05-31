package torrentlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPieceCountStats(t *testing.T) {
	tests := []struct {
		desc           string
		receivedPieces []int
		summary        *receivedPiecesSummary
		err            error
	}{
		{"empty array", []int{}, nil, errEmptyReceivedPieces},
		{"negative in received", []int{0, 1, -1}, nil, errNegativeReceivedPieces},
		{"all zeroes", []int{0, 0, 0}, &receivedPiecesSummary{3, 0, 0, 0.0, 0.0}, nil},
		{"all positive", []int{2, 6, 1}, &receivedPiecesSummary{0, 1, 6, 3.0, 2.6457513110645907}, nil},
		{"mixed zero and positive", []int{1, 0, 2}, &receivedPiecesSummary{1, 0, 2, 1.0, 1.0}, nil},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			summary, err := newReceivedPiecesSummary(test.receivedPieces)
			require.Equal(t, test.err, err)
			require.Equal(t, test.summary, summary)
		})
	}
}
