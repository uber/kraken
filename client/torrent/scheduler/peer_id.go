package scheduler

import (
	"bytes"
	"encoding/hex"
	"errors"
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
