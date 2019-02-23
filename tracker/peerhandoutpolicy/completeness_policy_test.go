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
	"math/rand"
	"testing"

	"github.com/uber/kraken/core"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestCompletenessPriorityPolicy(t *testing.T) {
	require := require.New(t)

	policy, err := NewPriorityPolicy(tally.NoopScope, _completenessPolicy)
	require.NoError(err)

	seeders := 10
	origins := 3
	incomplete := 20

	peers := make([]*core.PeerInfo, seeders+origins+incomplete)
	for k := 0; k < len(peers); k++ {
		p := core.PeerInfoFixture()
		if k < seeders {
			p.Complete = true
		} else if k < origins {
			p.Complete = true
			p.Origin = true
		}
		peers[k] = p
	}

	// shuffle
	for i := 0; i < len(peers); i++ {
		j := rand.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}

	policy.SortPeers(core.PeerInfoFixture(), peers)
	require.Len(peers, seeders+origins+incomplete)
	for k := 0; k < len(peers); k++ {
		p := peers[k]
		if k < seeders {
			require.True(p.Complete)
			require.False(p.Origin)
		} else if k < origins {
			require.True(p.Complete)
			require.True(p.Origin)
		} else {
			require.False(p.Complete)
			require.False(p.Origin)
		}
	}
}
