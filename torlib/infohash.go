package torlib

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
)

// InfoHash is 20-byte SHA1 Infohash used for info and pieces.
type InfoHash [20]byte

// Bytes retutns byte representation of a Infohash
func (h InfoHash) Bytes() []byte {
	return h[:]
}

// AsString casts byte array to string
func (h InfoHash) AsString() string {
	return string(h[:])
}

// String converts byte array into a hash string
// formatting convinience functionx
func (h InfoHash) String() string {
	return h.HexString()
}

// HexString converts a byte array into hexidemical string
func (h InfoHash) HexString() string {
	return fmt.Sprintf("%x", h[:])
}

// NewInfoHashFromHex converts a hexidemical string into a InfoHash
func NewInfoHashFromHex(s string) (h InfoHash, err error) {
	if len(s) != 40 {
		err = fmt.Errorf("InfoHash hex string has bad length: %d", len(s))
		return
	}
	n, err := hex.Decode(h[:], []byte(s))
	if err != nil {
		return
	}
	if n != 20 {
		panic(n)
	}
	return
}

// NewInfoHashFromBytes does a Infohash checksum for input array
func NewInfoHashFromBytes(b []byte) (h InfoHash) {
	hasher := sha1.New()
	hasher.Write(b)
	copy(h[:], hasher.Sum(nil))
	return
}
