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
package peerhandoutpolicy

import "github.com/uber/kraken/core"

const _completenessPolicy = "completeness"

// completenessAssignmentPolicy assigns priorities based on download completeness.
// Peers who've completed downloading are highest, then origins, then other peers.
type completenessAssignmentPolicy struct{}

func newCompletenessAssignmentPolicy() assignmentPolicy {
	return &completenessAssignmentPolicy{}
}

func (p *completenessAssignmentPolicy) assignPriority(peer *core.PeerInfo) (int, string) {
	if peer.Origin {
		return 1, "origin"
	}
	if peer.Complete {
		return 0, "peer_seeder"
	}
	return 2, "peer_incomplete"
}
