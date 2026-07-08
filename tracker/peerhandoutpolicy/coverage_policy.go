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
package peerhandoutpolicy

import (
	"github.com/uber/kraken/core"

	"github.com/willf/bitset"
)

const _coveragePolicy = "coverage"

// coverageAssignmentPolicy ranks a peer that covers at least one of the
// announcing peer's currently-requested pieces (V3 announce, streaming
// dispatchers) ahead of an otherwise-equal incomplete peer. Falls back to
// completenessAssignmentPolicy's ordering when requested is empty or a
// peer's bitfield is unavailable (pre-V3 peer, or a peer that hasn't
// announced with a bitfield yet).
type coverageAssignmentPolicy struct{}

func newCoverageAssignmentPolicy() assignmentPolicy {
	return &coverageAssignmentPolicy{}
}

func (p *coverageAssignmentPolicy) assignPriority(peer *core.PeerInfo, requested []int) (int, string) {
	if peer.Origin {
		return 0, "origin"
	}
	if peer.Complete {
		return 1, "peer_seeder"
	}
	if len(requested) > 0 && peerCovers(peer, requested) {
		return 2, "peer_partial_covering"
	}
	return 3, "peer_incomplete"
}

// peerCovers reports whether peer's bitfield has at least one of the
// requested piece indices set.
func peerCovers(peer *core.PeerInfo, requested []int) bool {
	if len(peer.Bitfield) == 0 {
		return false
	}
	b := &bitset.BitSet{}
	if err := b.UnmarshalBinary(peer.Bitfield); err != nil {
		return false
	}
	for _, i := range requested {
		if i >= 0 && b.Test(uint(i)) {
			return true
		}
	}
	return false
}
