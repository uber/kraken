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
	"testing"

	"github.com/uber/kraken/core"

	"github.com/stretchr/testify/require"
)

func TestPriorityPolicyRemoveSource(t *testing.T) {
	require := require.New(t)

	policy := DefaultPriorityPolicyFixture()

	src := core.PeerInfoFixture()
	peers := make([]*core.PeerInfo, 10)
	for k := 0; k < len(peers); k++ {
		peers[k] = core.PeerInfoFixture()
	}
	peers = append(peers, src)

	sorted := policy.SortPeers(src, peers)
	require.Len(sorted, len(peers)-1)
	for k := 0; k < len(sorted); k++ {
		require.NotEqual(src, sorted[k])
	}
}
