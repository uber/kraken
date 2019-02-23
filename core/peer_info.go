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

import "sort"

// PeerInfo defines peer metadata scoped to a torrent.
type PeerInfo struct {
	PeerID   PeerID `json:"peer_id"`
	IP       string `json:"ip"`
	Port     int    `json:"port"`
	Origin   bool   `json:"origin"`
	Complete bool   `json:"complete"`
}

// NewPeerInfo creates a new PeerInfo.
func NewPeerInfo(
	peerID PeerID,
	ip string,
	port int,
	origin bool,
	complete bool) *PeerInfo {

	return &PeerInfo{
		PeerID:   peerID,
		IP:       ip,
		Port:     port,
		Origin:   origin,
		Complete: complete,
	}
}

// PeerInfoFromContext derives PeerInfo from a PeerContext.
func PeerInfoFromContext(pctx PeerContext, complete bool) *PeerInfo {
	return NewPeerInfo(pctx.PeerID, pctx.IP, pctx.Port, pctx.Origin, complete)
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
