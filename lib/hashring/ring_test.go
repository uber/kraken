package hashring

import (
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/stretchr/testify/require"
)

func TestRingDistribution(t *testing.T) {
	tests := []struct {
		desc                 string
		clusterSize          int
		maxReplica           int
		expectedDistribution float64
	}{
		{"single host", 1, 1, 1.0},
		{"all replicas", 3, 3, 1.0},
		{"below max replica", 2, 3, 1.0},
		{"above max replica", 6, 3, 0.5},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			var addrs []string
			for i := 0; i < test.clusterSize; i++ {
				addrs = append(addrs, randutil.Addr())
			}
			r, err := New(Config{MaxReplica: test.maxReplica}, hostlist.Fixture(addrs...))
			require.NoError(err)

			sampleSize := 2000

			counts := make(map[string]int)
			for i := 0; i < sampleSize; i++ {
				for _, addr := range r.Locations(core.DigestFixture()) {
					counts[addr]++
				}
			}

			for _, addr := range addrs {
				distribution := float64(counts[addr]) / float64(sampleSize)
				require.InDelta(test.expectedDistribution, distribution, 0.05)
			}
		})
	}
}

func TestRingContains(t *testing.T) {
	require := require.New(t)

	x := "x:80"
	y := "y:80"
	z := "z:80"

	r, err := New(Config{}, hostlist.Fixture(x, y))
	require.NoError(err)

	require.True(r.Contains(x))
	require.True(r.Contains(y))
	require.False(r.Contains(z))
}
