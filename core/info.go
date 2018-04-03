package core

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	bencode "github.com/jackpal/bencode-go"
)

// Info is a torrent info dictionary.
type Info struct {
	PieceLength int64
	PieceSums   []uint32
	Name        string
	Length      int64
}

// NewInfoFromFile creates new info given file and piecelength
func NewInfoFromFile(name, filepath string, pieceLength int64) (Info, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return Info{}, fmt.Errorf("open file: %s", err)
	}
	defer f.Close()

	return NewInfoFromBlob(name, f, pieceLength)
}

// NewInfoFromBlob creates a new Info from a blob.
func NewInfoFromBlob(name string, blob io.Reader, pieceLength int64) (Info, error) {
	length, pieceSums, err := calcPieceSums(blob, pieceLength)
	if err != nil {
		return Info{}, fmt.Errorf("calc piece sums: %s", err)
	}
	return Info{
		PieceLength: pieceLength,
		PieceSums:   pieceSums,
		Name:        name,
		Length:      length,
	}, nil
}

// TotalLength returns a total length of all torrent files
func (info *Info) TotalLength() (ret int64) {
	return info.Length
}

// NumPieces return number of pieces in a torrent
func (info *Info) NumPieces() int {
	return len(info.PieceSums)
}

// ComputeInfoHash returns the hash of Info
// it is an identifier of a torrent
func (info *Info) ComputeInfoHash() (InfoHash, error) {
	b := new(bytes.Buffer)
	if err := bencode.Marshal(b, *info); err != nil {
		return InfoHash{}, err
	}
	return NewInfoHashFromBytes(b.Bytes()), nil
}

// Serialize returns info as bytes
func (info *Info) Serialize() ([]byte, error) {
	return json.Marshal(info)
}

// GetPieceLength returns the length of piece i.
func (info *Info) GetPieceLength(i int) int64 {
	if i < 0 || i >= len(info.PieceSums) {
		return 0
	}
	if i == len(info.PieceSums)-1 {
		// Last piece.
		return info.Length - info.PieceLength*int64(i)
	}
	return info.PieceLength
}

// pieceHashSize returns the size of each piece hash.
func (info *Info) pieceHashSize() int {
	return sha1.New().Size()
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
