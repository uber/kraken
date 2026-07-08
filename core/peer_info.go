// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package core

import (
	"sort"

	"github.com/willf/bitset"
)

// PeerInfo defines peer metadata scoped to a torrent.
type PeerInfo struct {
	PeerID   PeerID `json:"peer_id"`
	IP       string `json:"ip"`
	Port     int    `json:"port"`
	Origin   bool   `json:"origin"`
	Complete bool   `json:"complete"`
	// Bitfield is the peer's per-piece have-set, packed via bitset.BitSet's
	// own MarshalBinary -- the same encoding agent-to-agent handshakes
	// already use for bitfields. nil unless set via WithBitfield.
	Bitfield []byte `json:"bitfield,omitempty"`
	// NumComplete is a cheap progress summary (== set-bit count; 0 if nil).
	NumComplete int `json:"num_complete,omitempty"`
}

// PeerInfoOption sets an optional PeerInfo field.
type PeerInfoOption func(*PeerInfo)

// WithBitfield attaches a packed snapshot of b, plus its set-bit count, to a
// PeerInfo under construction.
func WithBitfield(b *bitset.BitSet) PeerInfoOption {
	return func(p *PeerInfo) {
		encoded, err := b.MarshalBinary()
		if err != nil {
			return
		}
		p.Bitfield = encoded
		p.NumComplete = int(b.Count())
	}
}

// NewPeerInfo creates a new PeerInfo.
func NewPeerInfo(
	peerID PeerID,
	ip string,
	port int,
	origin bool,
	complete bool,
	opts ...PeerInfoOption) *PeerInfo {

	p := &PeerInfo{
		PeerID:   peerID,
		IP:       ip,
		Port:     port,
		Origin:   origin,
		Complete: complete,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// PeerInfoFromContext derives PeerInfo from a PeerContext.
func PeerInfoFromContext(pctx PeerContext, complete bool, opts ...PeerInfoOption) *PeerInfo {
	return NewPeerInfo(pctx.PeerID, pctx.IP, pctx.Port, pctx.Origin, complete, opts...)
}

// PeerInfos groups PeerInfo structs for sorting.
type PeerInfos []*PeerInfo

// Len for sorting.
func (s PeerInfos) Len() int { return len(s) }

// Swap for sorting
func (s PeerInfos) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// PeersByPeerID sorts PeerInfos by peer id.
type PeersByPeerID struct{ PeerInfos }

// Less for sorting.
func (s PeersByPeerID) Less(i, j int) bool {
	return s.PeerInfos[i].PeerID.LessThan(s.PeerInfos[j].PeerID)
}

// SortedByPeerID returns a copy of peers which has been sorted by peer id.
func SortedByPeerID(peers []*PeerInfo) []*PeerInfo {
	c := make([]*PeerInfo, len(peers))
	copy(c, peers)
	sort.Sort(PeersByPeerID{PeerInfos(c)})
	return c
}
