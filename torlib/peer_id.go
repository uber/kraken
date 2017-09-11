package torlib

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
)

// ErrInvalidPeerIDLength returns when a string peer id does not decode into 20 bytes.
var ErrInvalidPeerIDLength = errors.New("peer id has invalid length")

// PeerID represents a fixed size peer id.
type PeerID [20]byte

// NewPeerID parses a PeerID from the given string. Must be in hexadecimal notation,
// encoding exactly 20 bytes.
func NewPeerID(s string) (PeerID, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return PeerID{}, err
	}
	if len(b) != 20 {
		return PeerID{}, ErrInvalidPeerIDLength
	}
	var p PeerID
	copy(p[:], b)
	return p, nil
}

// String encodes the PeerID in hexadecimal notation.
func (p PeerID) String() string {
	return hex.EncodeToString(p[:])
}

// LessThan returns whether p is less than o.
func (p PeerID) LessThan(o PeerID) bool {
	return bytes.Compare(p[:], o[:]) == -1
}

// RandomPeerID returns a randomly generated PeerID.
func RandomPeerID() (PeerID, error) {
	var p PeerID
	_, err := rand.Read(p[:])
	return p, err
}

// HashedPeerID returns a PeerID derived from the hash of s.
func HashedPeerID(s string) (PeerID, error) {
	var p PeerID
	if s == "" {
		return p, errors.New("cannot generate peer id from empty string")
	}
	h := sha1.New()
	io.WriteString(h, s)
	copy(p[:], h.Sum(nil))
	return p, nil
}
