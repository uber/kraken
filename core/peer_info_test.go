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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortedByPeerID(t *testing.T) {
	require := require.New(t)

	p1 := PeerInfoFixture()
	p2 := PeerInfoFixture()
	p3 := PeerInfoFixture()

	sorted := SortedByPeerID([]*PeerInfo{p1, p2, p3})
	require.True(sorted[0].PeerID.LessThan(sorted[1].PeerID))
	require.True(sorted[1].PeerID.LessThan(sorted[2].PeerID))
}
