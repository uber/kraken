package meta

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
)

// Hash is 20-byte SHA1 hash used for info and pieces.
type Hash [20]byte

// Bytes retutns byte representation of a hash
func (h Hash) Bytes() []byte {
	return h[:]
}

// AsString casts byte array to string
func (h Hash) AsString() string {
	return string(h[:])
}

// String converts byte array into a hash string
// formatting convinience functionx
func (h Hash) String() string {
	return h.HexString()
}

// HexString converts a byte array into hexidemical string
func (h Hash) HexString() string {
	return fmt.Sprintf("%x", h[:])
}

// FromHexString converts a hexidemical string into a Hash
func (h *Hash) FromHexString(s string) (err error) {
	if len(s) != 40 {
		err = fmt.Errorf("hash hex string has bad length: %d", len(s))
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

// NewHashFromHex converts a hexidemical string into a Hash
func NewHashFromHex(s string) (h Hash) {
	err := h.FromHexString(s)
	if err != nil {
		panic(err)
	}
	return
}

// HashBytes does a hash checksum for input array
func HashBytes(b []byte) (ret Hash) {
	hasher := sha1.New()
	hasher.Write(b)
	copy(ret[:], hasher.Sum(nil))
	return
}
