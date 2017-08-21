package torlib

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken-torrent/bencode"
)

// Info is a torrent info dictionary.
type Info struct {
	PieceLength int64  `bencode:"piece length"`
	Pieces      []byte `bencode:"pieces"`
	Name        string `bencode:"name"`
	Length      int64  `bencode:"length"`
}

// NewInfoFromFile creates new info given file and piecelength
func NewInfoFromFile(name, filepath string, pieceLength int64) (Info, error) {
	length, pieces, err := generatePieces(filepath, pieceLength)
	if err != nil {
		return Info{}, err
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
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		return InfoHash{}, err
	}
	return NewInfoHashFromBytes(infoBytes), nil
}

// Serialize returns info as bytes
func (info *Info) Serialize() ([]byte, error) {
	bytes, err := bencode.Marshal(info)
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

// generatePieces hashes file content in chunks given path and pieceLength, and returns file length and hashes
func generatePieces(fp string, pieceLength int64) (int64, []byte, error) {
	if pieceLength <= 0 {
		return 0, nil, errors.New("piece length must be positive")
	}

	f, err := os.Open(fp)
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return 0, nil, err
	}

	// Pipe file content
	pr, pw := io.Pipe()
	defer pr.Close()
	go func() {
		_, err := io.Copy(pw, f)
		pw.CloseWithError(err)
	}()

	// Generate hash
	var pieces []byte
	for {
		hasher := sha1.New()
		wn, err := io.CopyN(hasher, pr, pieceLength)
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			return 0, nil, err
		}
		if wn == 0 {
			break
		}
		pieces = hasher.Sum(pieces)
		if wn < pieceLength {
			break
		}
	}

	return stat.Size(), pieces, nil
}
