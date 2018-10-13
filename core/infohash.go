package core

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
)

// InfoHash is 20-byte SHA1 hash of the Info struct. It is the authoritative
// identifier for a torrent.
type InfoHash [20]byte

// NewInfoHashFromHex converts a hexidemical string into an InfoHash
func NewInfoHashFromHex(s string) (InfoHash, error) {
	if len(s) != 40 {
		return InfoHash{}, fmt.Errorf("invalid hash: expected 40 characters, got %d", len(s))
	}
	var h InfoHash
	n, err := hex.Decode(h[:], []byte(s))
	if err != nil {
		return InfoHash{}, fmt.Errorf("invalid hex: %s", err)
	}
	if n != 20 {
		return InfoHash{}, fmt.Errorf("invariant violation: expected 20 bytes, got %d", n)
	}
	return h, nil
}

// NewInfoHashFromBytes converts raw bytes to an InfoHash.
func NewInfoHashFromBytes(b []byte) InfoHash {
	var h InfoHash
	hasher := sha1.New()
	hasher.Write(b)
	copy(h[:], hasher.Sum(nil))
	return h
}

// Bytes converts h to raw bytes.
func (h InfoHash) Bytes() []byte {
	return h[:]
}

// Hex converts h into a hexidemical string.
func (h InfoHash) Hex() string {
	return hex.EncodeToString(h[:])
}

func (h InfoHash) String() string {
	return h.Hex()
}
