package torlib

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"code.uber.internal/go-common.git/x/log"
	bencode "github.com/jackpal/bencode-go"
)

// Info is a torrent info dictionary.
type Info struct {
	PieceLength int64  `bencode:"piece length"`
	Pieces      Pieces `bencode:"pieces"`
	Name        string `bencode:"name"`
	Length      int64  `bencode:"length"`
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
	length, pieces, err := generatePieces(blob, pieceLength)
	if err != nil {
		return Info{}, fmt.Errorf("generate pieces: %s", err)
	}
	return Info{
		PieceLength: pieceLength,
		Pieces:      pieces,
		Name:        name,
		Length:      length,
	}, nil
}

// PieceHash returns the hash for given piece
func (info *Info) PieceHash(piece int) ([]byte, error) {
	if piece >= info.NumPieces() {
		return nil, fmt.Errorf("Piece index %d out of range %d", piece, info.NumPieces())
	}

	if math.Mod(float64(len(info.Pieces)), float64(info.NumPieces())) != 0.0 {
		return nil, fmt.Errorf("Length of piece hash %d is not a muliple of the number of pieces %d",
			len(info.Pieces), info.NumPieces())
	}

	pieceHashSize := info.pieceHashSize()
	start := piece * pieceHashSize
	end := (piece + 1) * pieceHashSize
	hash := make([]byte, end-start)
	copy(hash, info.Pieces[start:end])
	return hash, nil
}

// TotalLength returns a total length of all torrent files
func (info *Info) TotalLength() (ret int64) {
	return info.Length
}

// NumPieces return number of pieces in a torrent
func (info *Info) NumPieces() int {
	if len(info.Pieces)%20 != 0 {
		panic(len(info.Pieces))
	}
	return len(info.Pieces) / 20
}

// Validate returns error if the Info is invalid.
func (info *Info) Validate() error {
	if len(info.Pieces)%20 != 0 {
		return errors.New("pieces has invalid length")
	}
	if info.PieceLength == 0 {
		if info.TotalLength() != 0 {
			return errors.New("zero piece length")
		}
	} else {
		if int((info.TotalLength()+info.PieceLength-1)/info.PieceLength) != info.NumPieces() {
			return fmt.Errorf("piece count and file lengths are at odds: num pieces %d", info.NumPieces())
		}
	}
	return nil
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
	bytes, err := json.Marshal(info)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return bytes, nil
}

// pieceHashSize returns the size of each piece hash.
func (info *Info) pieceHashSize() int {
	return sha1.New().Size()
}

// generatePieces hashes blob content in pieceLength chunks.
func generatePieces(blob io.Reader, pieceLength int64) (length int64, pieces Pieces, err error) {
	if pieceLength <= 0 {
		return 0, nil, errors.New("piece length must be positive")
	}
	for {
		h := sha1.New()
		n, err := io.CopyN(h, blob, pieceLength)
		if err != nil && err != io.EOF {
			return 0, nil, fmt.Errorf("read blob: %s", err)
		}
		length += n
		if n == 0 {
			break
		}
		pieces = h.Sum(pieces)
		if n < pieceLength {
			break
		}
	}
	return length, pieces, nil
}
