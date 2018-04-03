package storage

import (
	"fmt"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/bitsetutil"

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
			info := NewTorrentInfo(mi, test.bitfield)
			require.Equal(t, test.expected, info.PercentDownloaded())
		})
	}
}
