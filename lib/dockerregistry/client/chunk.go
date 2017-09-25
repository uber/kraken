package client

import (
	"fmt"
	"math"
)

type blobChunk struct {
	size      int64
	chunkSize int64
}

func (c blobChunk) numChunks() (int, error) {
	if c.chunkSize <= 0 {
		return 0, fmt.Errorf("invalid chunksize: %d", c.chunkSize)
	}

	n := math.Ceil(float64(c.size) / float64(c.chunkSize))
	return int(n), nil
}

func (c blobChunk) getChunkStartEnd(index int) (start int64, end int64, err error) {
	if c.chunkSize <= 0 {
		return 0, 0, fmt.Errorf("invalid chunksize: %d", c.chunkSize)
	}

	start = int64(index) * c.chunkSize
	end = start + c.chunkSize

	if index < 0 || start >= c.size {
		return 0, 0, fmt.Errorf("invalid index: %d", index)
	}

	if end >= c.size {
		end = c.size
	}
	return start, end, nil
}
