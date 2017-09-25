package client

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunk(t *testing.T) {
	testCases := []struct {
		size              int64
		chunkSize         int64
		index             int
		expectedNumChunks int
		expectedStart     int64
		expectedEnd       int64
	}{
		{0, 0, 0, 0, 0, 0},
		{0, 1, 0, 0, 0, 0},
		{0, 1, 1, 0, 0, 0},
		{0, 1, -1, 0, 0, 0},
		{1, 0, 0, 0, 0, 0},
		{1, 1, 0, 1, 0, 1},
		{2, 2, 0, 1, 0, 2},
		{2, 2, 1, 1, 0, 0},
		{2, 1, 0, 2, 0, 1},
		{2, 1, 1, 2, 1, 2},
		{4, 2, 0, 2, 0, 2},
		{4, 2, 1, 2, 2, 4},
		{4, 2, 2, 2, 0, 0},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("TestChunk %v", test), func(t *testing.T) {
			require := require.New(t)
			c := blobChunk{test.size, test.chunkSize}
			n, _ := c.numChunks()
			start, end, _ := c.getChunkStartEnd(test.index)
			require.Equal(test.expectedNumChunks, n)
			require.Equal(test.expectedStart, start)
			require.Equal(test.expectedEnd, end)
		})
	}
}
