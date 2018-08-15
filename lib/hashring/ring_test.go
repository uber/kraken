package hashring

import (
	"errors"
	"testing"
	"time"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/healthcheck"
	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/mocks/lib/hostlist"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/stringset"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func addrsFixture(n int) []string {
	var addrs []string
	for i := 0; i < n; i++ {
		addrs = append(addrs, randutil.Addr())
	}
	return addrs
}

func TestRingLocationsDistribution(t *testing.T) {
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

			addrs := addrsFixture(test.clusterSize)

			r, err := New(
				Config{MaxReplica: test.maxReplica},
				hostlist.Fixture(addrs...),
				healthcheck.IdentityFilter{})
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

func TestRingLocationsFiltersOutUnhealthyHosts(t *testing.T) {
	require := require.New(t)

	filter := healthcheck.NewManualFilter()

	r, err := New(
		Config{MaxReplica: 3},
		hostlist.Fixture(addrsFixture(10)...),
		filter)
	require.NoError(err)

	d := core.DigestFixture()

	replicas := r.Locations(d)
	require.Len(replicas, 3)

	filter.Unhealthy.Add(replicas[0])
	require.NoError(r.Refresh())

	result := r.Locations(d)
	require.Equal(replicas[1:], result)
}

func TestRingLocationsReturnsNextHealthyHostWhenReplicaSetUnhealthy(t *testing.T) {
	require := require.New(t)

	filter := healthcheck.NewManualFilter()

	r, err := New(
		Config{MaxReplica: 3},
		hostlist.Fixture(addrsFixture(10)...),
		filter)
	require.NoError(err)

	d := core.DigestFixture()

	replicas := r.Locations(d)
	require.Len(replicas, 3)

	// Mark all the original replicas as unhealthy.
	for _, addr := range replicas {
		filter.Unhealthy.Add(addr)
	}
	require.NoError(r.Refresh())

	// Should consistently select the next address.
	var next []string
	for i := 0; i < 10; i++ {
		next = r.Locations(d)
		require.Len(next, 1)
		require.NotContains(replicas, next[0])
	}

	// Mark the next address as unhealthy.
	filter.Unhealthy.Add(next[0])
	require.NoError(r.Refresh())

	// Should consistently select the address after next.
	for i := 0; i < 10; i++ {
		nextNext := r.Locations(d)
		require.Len(nextNext, 1)
		require.NotContains(append(replicas, next[0]), nextNext[0])
	}
}

func TestRingLocationsReturnsFirstHostWhenAllHostsUnhealthy(t *testing.T) {
	require := require.New(t)

	filter := healthcheck.NewBinaryFilter()

	r, err := New(
		Config{MaxReplica: 3},
		hostlist.Fixture(addrsFixture(10)...),
		filter)
	require.NoError(err)

	d := core.DigestFixture()

	replicas := r.Locations(d)
	require.Len(replicas, 3)

	filter.Healthy = false
	require.NoError(r.Refresh())

	// Should consistently select the first replica once all are unhealthy.
	for i := 0; i < 10; i++ {
		result := r.Locations(d)
		require.Len(result, 1)
		require.Equal(replicas[0], result[0])
	}
}

func TestRingContains(t *testing.T) {
	require := require.New(t)

	x := "x:80"
	y := "y:80"
	z := "z:80"

	r, err := New(Config{}, hostlist.Fixture(x, y), healthcheck.IdentityFilter{})
	require.NoError(err)

	require.True(r.Contains(x))
	require.True(r.Contains(y))
	require.False(r.Contains(z))
}

func TestRingMonitor(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cluster := mockhostlist.NewMockList(ctrl)

	x := "x:80"
	y := "y:80"

	gomock.InOrder(
		cluster.EXPECT().Resolve().Return(stringset.New(x), nil),
		cluster.EXPECT().Resolve().Return(stringset.New(y), nil),
	)

	r, err := New(
		Config{RefreshInterval: time.Second},
		cluster,
		healthcheck.IdentityFilter{})
	require.NoError(err)

	stop := make(chan struct{})
	defer close(stop)
	go r.Monitor(stop)

	d := core.DigestFixture()

	require.Equal([]string{x}, r.Locations(d))

	// Monitor should refresh the ring.
	time.Sleep(1250 * time.Millisecond)

	require.Equal([]string{y}, r.Locations(d))
}

func TestRingRefreshErrors(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cluster := mockhostlist.NewMockList(ctrl)

	filter := healthcheck.NewManualFilter()

	x := "x:80"
	y := "y:80"

	gomock.InOrder(
		cluster.EXPECT().Resolve().Return(stringset.New(x, y), nil),
		cluster.EXPECT().Resolve().Return(nil, errors.New("some error")),
	)

	r, err := New(Config{}, cluster, filter)
	require.NoError(err)

	d := core.DigestFixture()

	require.ElementsMatch([]string{x, y}, r.Locations(d))

	filter.Unhealthy.Add(x)

	// Refresh should be resilient to the 2nd resolve error and can still run
	// health checks.
	require.NoError(r.Refresh())

	require.Equal([]string{y}, r.Locations(d))
}

func TestRingRefreshUpdatesMembership(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cluster := mockhostlist.NewMockList(ctrl)

	x := "x:80"
	y := "y:80"
	z := "z:80"

	// x is removed and z is added on the 2nd resolve.
	gomock.InOrder(
		cluster.EXPECT().Resolve().Return(stringset.New(x, y), nil),
		cluster.EXPECT().Resolve().Return(stringset.New(y, z), nil),
	)

	r, err := New(Config{}, cluster, healthcheck.IdentityFilter{})
	require.NoError(err)

	d := core.DigestFixture()

	require.ElementsMatch([]string{x, y}, r.Locations(d))

	require.NoError(r.Refresh())

	require.ElementsMatch([]string{y, z}, r.Locations(d))
}
