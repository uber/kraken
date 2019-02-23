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
package storage

import (
	"fmt"
	"testing"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/bitsetutil"

	"github.com/stretchr/testify/require"
	"github.com/willf/bitset"
)

func TestTorrentInfoPercentDownloaded(t *testing.T) {
	mi := core.SizedBlobFixture(100, 25).MetaInfo
	tests := []struct {
		bitfield *bitset.BitSet
		expected int
	}{
		{bitsetutil.FromBools(true, true, true, true), 100},
		{bitsetutil.FromBools(true, false, true, false), 50},
		{bitsetutil.FromBools(false, false, false, false), 0},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%d%%", test.expected), func(t *testing.T) {
			require := require.New(t)

			info := NewTorrentInfo(mi, test.bitfield)
			require.Equal(test.expected, info.PercentDownloaded())
			require.Equal(test.bitfield, info.Bitfield())
			require.Equal(int64(25), info.MaxPieceLength())
			require.Equal(mi.InfoHash(), info.InfoHash())
			require.Equal(mi.Digest(), info.Digest())
			require.Equal(mi.InfoHash().Hex(), info.String())
		})
	}
}
