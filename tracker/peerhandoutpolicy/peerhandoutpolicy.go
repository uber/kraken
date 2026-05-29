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
	"fmt"
	"sort"

	"github.com/uber-go/tally"

	"github.com/uber/kraken/core"
)

// On the client-side, agent leeches only from the top N seeders.
const _maxSeedersUsedPerTorrent = 10

var _numSeedersHistogramBuckets = tally.MustMakeLinearValueBuckets(0, 1, 20)

type peerPriorityInfo struct {
	peer     *core.PeerInfo
	priority int
	label    string
}

// assignmentPolicy defines the policy for assigning priority to peers.
type assignmentPolicy interface {
	assignPriority(peer *core.PeerInfo) (priority int, label string)
}

// PriorityPolicy wraps an assignmentPolicy and uses it to sort lists of peers.
type PriorityPolicy struct {
	stats  tally.Scope
	policy assignmentPolicy
}

// NewPriorityPolicy returns a PriorityPolicy that assigns priorities using the given priority policy.
func NewPriorityPolicy(stats tally.Scope, priorityPolicy string) (*PriorityPolicy, error) {
	p := &PriorityPolicy{
		stats: stats.Tagged(map[string]string{
			"module":   "peerhandoutpolicy",
			"priority": priorityPolicy,
		}),
	}

	switch priorityPolicy {
	case _defaultPolicy:
		p.policy = newDefaultAssignmentPolicy()
	case _completenessPolicy:
		p.policy = newCompletenessAssignmentPolicy()
	default:
		return nil, fmt.Errorf("priority policy %q not found", priorityPolicy)
	}

	return p, nil
}

// SortPeers returns the given list of peers sorted by the priority assigned to them
// by the priorityPolicy. Excludes the source peer from the list.
func (p *PriorityPolicy) SortPeers(source *core.PeerInfo, peers []*core.PeerInfo) []*core.PeerInfo {
	peerPriorities := make([]*peerPriorityInfo, 0, len(peers))
	for _, peer := range peers {
		if peer == source {
			continue
		}
		priority, label := p.policy.assignPriority(peer)
		peerPriorities = append(peerPriorities, &peerPriorityInfo{peer, priority, label})
	}

	sort.Slice(peerPriorities, func(i, j int) bool {
		return peerPriorities[i].priority < peerPriorities[j].priority
	})

	p.emitNumSeeders(peerPriorities)

	sortedPeers := []*core.PeerInfo{}
	for _, peerPrio := range peerPriorities {
		sortedPeers = append(sortedPeers, peerPrio.peer)
	}

	return sortedPeers
}

func (p *PriorityPolicy) emitNumSeeders(peerPriorities []*peerPriorityInfo) {
	if len(peerPriorities) > _maxSeedersUsedPerTorrent {
		peerPriorities = peerPriorities[:_maxSeedersUsedPerTorrent]
	}

	numOrigins, numCompleteAgents, numIncompleteAgents := 0, 0, 0
	for _, peerPrio := range peerPriorities {
		switch peerPrio.label {
		case "peer_seeder":
			numCompleteAgents++
		case "peer_incomplete":
			numIncompleteAgents++
		case "origin":
			numOrigins++
		}
	}

	total := numOrigins + numCompleteAgents + numIncompleteAgents
	p.stats.Histogram("total_seeders", _numSeedersHistogramBuckets).RecordValue(float64(total))
	p.stats.Histogram("origin_seeders", _numSeedersHistogramBuckets).RecordValue(float64(numOrigins))
	p.stats.Histogram("complete_agent_seeders", _numSeedersHistogramBuckets).RecordValue(float64(numCompleteAgents))
	p.stats.Histogram("incomplete_agent_seeders", _numSeedersHistogramBuckets).RecordValue(float64(numIncompleteAgents))
}
