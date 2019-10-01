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

import (
	"fmt"
	"sort"

	"github.com/uber-go/tally"

	"github.com/uber/kraken/core"
)

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
	for k := 0; k < len(peers); k++ {
		if peers[k] != source {
			priority, label := p.policy.assignPriority(peers[k])
			peerPriorities = append(peerPriorities,
				&peerPriorityInfo{peers[k], priority, label})
		}
	}

	sort.Slice(peerPriorities, func(i, j int) bool {
		return peerPriorities[i].priority < peerPriorities[j].priority
	})

	priorityCounts := make(map[string]int)
	for k := 0; k < len(peerPriorities); k++ {
		p := peerPriorities[k]
		peers[k] = p.peer
		if _, ok := priorityCounts[p.label]; ok {
			priorityCounts[p.label]++
		} else {
			priorityCounts[p.label] = 1
		}
	}
	peers = peers[:len(peerPriorities)]

	for label, count := range priorityCounts {
		p.stats.Tagged(map[string]string{
			"label": label,
		}).Gauge("count").Update(float64(count))
	}

	return peers
}
