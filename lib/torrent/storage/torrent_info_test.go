package storage

import (
	"fmt"
	"testing"

	"code.uber.internal/infra/kraken/torlib"
	"github.com/stretchr/testify/require"
)

func TestTorrentInfoPercentDownloaded(t *testing.T) {
	mi := torlib.CustomMetaInfoFixture(100, 25)
	tests := []struct {
		bitfield Bitfield
		expected int
	}{
		{Bitfield{true, true, true, true}, 100},
		{Bitfield{true, false, true, false}, 50},
		{Bitfield{false, false, false, false}, 0},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%d%%", test.expected), func(t *testing.T) {
			info := newTorrentInfo(mi, test.bitfield)
			require.Equal(t, test.expected, info.PercentDownloaded())
		})
	}
}
