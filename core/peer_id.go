// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package core

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
)

// PeerIDFactory defines the method used to generate a peer id.
type PeerIDFactory string

// RandomPeerIDFactory creates random peer ids.
const RandomPeerIDFactory PeerIDFactory = "random"

// AddrHashPeerIDFactory creates peers ids based on a full "ip:port" address.
const AddrHashPeerIDFactory PeerIDFactory = "addr_hash"

// GeneratePeerID creates a new peer id per the factory policy.
func (f PeerIDFactory) GeneratePeerID(ip string, port int) (PeerID, error) {
	switch f {
	case RandomPeerIDFactory:
		return RandomPeerID()
	case AddrHashPeerIDFactory:
		return HashedPeerID(fmt.Sprintf("%s:%d", ip, port))
	default:
		err := fmt.Errorf("invalid peer id factory: %q", string(f))
		return PeerID{}, err
	}
}

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
