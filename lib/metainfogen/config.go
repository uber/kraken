package metainfogen

import (
	"errors"
	"sort"

	"github.com/c2h5oh/datasize"
)

// Config defines Generator configuration.
type Config struct {
	PieceLengths map[datasize.ByteSize]datasize.ByteSize `yaml:"piece_lengths"`
}

type rangeConfig struct {
	fileSize    int64
	pieceLength int64
}

// pieceLengthConfig represents a sorted list joining file size to torrent piece
// length for all files under said size, for example, these ranges:
//
//   [
//     (0, 1mb),
//     (2gb, 4mb),
//     (4gb, 8mb),
//   ]
//
// are interpreted as:
//
//   N < 2gb           : 1mb
//   N >= 2gb, N < 4gb : 4mb
//   N >= 4gb          : 8mb
//
type pieceLengthConfig struct {
	ranges []rangeConfig
}

func newPieceLengthConfig(
	pieceLengthByFileSize map[datasize.ByteSize]datasize.ByteSize) (*pieceLengthConfig, error) {

	if len(pieceLengthByFileSize) == 0 {
		return nil, errors.New("no piece lengths configured")
	}
	var ranges []rangeConfig
	for fileSize, pieceLength := range pieceLengthByFileSize {
		ranges = append(ranges, rangeConfig{
			fileSize:    int64(fileSize),
			pieceLength: int64(pieceLength),
		})
	}
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].fileSize < ranges[j].fileSize
	})
	return &pieceLengthConfig{ranges}, nil
}

func (c *pieceLengthConfig) get(fileSize int64) int64 {
	pieceLength := c.ranges[0].pieceLength
	for _, r := range c.ranges {
		if fileSize < r.fileSize {
			break
		}
		pieceLength = r.pieceLength
	}
	return pieceLength
}
