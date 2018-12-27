package hashring

import (
	"runtime"
	"testing"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/mocks/lib/hashring"
	"github.com/uber/kraken/mocks/lib/hostlist"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/stringset"

	"github.com/andres-erbsen/clock"
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
			monitor := healthcheck.NewMonitor(
				healthcheck.MonitorConfig{},
				hostlist.Fixture(addrs...),
				healthcheck.IdentityFilter{})

			r := New(
				Config{MaxReplica: test.maxReplica},
				monitor)

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

	clk := clock.NewMock()
	filter := healthcheck.NewManualFilter()
	config := healthcheck.MonitorConfig{Interval: 1 * time.Second}
	monitor := healthcheck.NewMonitor(
		config,
		hostlist.Fixture(addrsFixture(10)...),
		filter,
		healthcheck.WithClk(clk))
	runtime.Gosched()

	r := New(
		Config{MaxReplica: 3},
		monitor)

	d := core.DigestFixture()

	replicas := r.Locations(d)
	require.Len(replicas, 3)

	filter.Unhealthy.Add(replicas[0])
	clk.Add(config.Interval + config.Interval/3)
	r.Refresh()

	result := r.Locations(d)
	require.Equal(replicas[1:], result)
}

func TestRingLocationsReturnsNextHealthyHostWhenReplicaSetUnhealthy(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	filter := healthcheck.NewManualFilter()
	config := healthcheck.MonitorConfig{Interval: 1 * time.Second}
	monitor := healthcheck.NewMonitor(
		config,
		hostlist.Fixture(addrsFixture(10)...),
		filter,
		healthcheck.WithClk(clk))
	runtime.Gosched()

	r := New(
		Config{MaxReplica: 3},
		monitor)

	d := core.DigestFixture()

	replicas := r.Locations(d)
	require.Len(replicas, 3)

	// Mark all the original replicas as unhealthy.
	for _, addr := range replicas {
		filter.Unhealthy.Add(addr)
	}
	clk.Add(config.Interval + config.Interval/3)
	r.Refresh()

	// Should consistently select the next address.
	var next []string
	for i := 0; i < 10; i++ {
		next = r.Locations(d)
		require.Len(next, 1)
		require.NotContains(replicas, next[0])
	}

	// Mark the next address as unhealthy.
	filter.Unhealthy.Add(next[0])
	clk.Add(config.Interval + config.Interval/3)
	r.Refresh()

	// Should consistently select the address after next.
	for i := 0; i < 10; i++ {
		nextNext := r.Locations(d)
		require.Len(nextNext, 1)
		require.NotContains(append(replicas, next[0]), nextNext[0])
	}
}

func TestRingLocationsReturnsFirstHostWhenAllHostsUnhealthy(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	filter := healthcheck.NewBinaryFilter()
	config := healthcheck.MonitorConfig{Interval: 1 * time.Second}
	monitor := healthcheck.NewMonitor(
		config,
		hostlist.Fixture(addrsFixture(10)...),
		filter,
		healthcheck.WithClk(clk))
	runtime.Gosched()

	r := New(
		Config{MaxReplica: 3},
		monitor)

	d := core.DigestFixture()

	replicas := r.Locations(d)
	require.Len(replicas, 3)

	filter.Healthy = false
	clk.Add(config.Interval + config.Interval/3)
	r.Refresh()

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

	monitor := healthcheck.NewMonitor(
		healthcheck.MonitorConfig{},
		hostlist.Fixture(x, y),
		healthcheck.IdentityFilter{})

	r := New(Config{}, monitor)

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
		cluster.EXPECT().Resolve().Return(stringset.New(x)),
		cluster.EXPECT().Resolve().Return(stringset.New(y)),
	)

	clk := clock.NewMock()
	config := healthcheck.MonitorConfig{Interval: 1 * time.Second}
	monitor := healthcheck.NewMonitor(
		config,
		cluster,
		healthcheck.IdentityFilter{},
		healthcheck.WithClk(clk))
	runtime.Gosched()

	r := New(
		Config{},
		monitor)

	d := core.DigestFixture()

	require.Equal([]string{x}, r.Locations(d))

	// Monitor should refresh the ring.
	clk.Add(config.Interval + config.Interval/3)

	require.Equal([]string{y}, r.Locations(d))
}

func TestRingPassive(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()
	config := healthcheck.PassiveConfig{Fails: 1, FailTimeout: 1 * time.Second}
	p := healthcheck.NewPassive(
		config,
		clk,
		hostlist.Fixture(addrsFixture(10)...))

	r := New(
		Config{MaxReplica: 3},
		p)

	d := core.DigestFixture()

	replicas := r.Locations(d)
	require.Len(replicas, 3)

	r.Failed(replicas[0])
	r.Refresh()

	result := r.Locations(d)
	require.Equal(replicas[1:], result)

	// Refresh should remove host from failed list due to timeout.
	clk.Add(config.FailTimeout + config.FailTimeout/3)
	r.Refresh()

	result = r.Locations(d)
	require.Equal(replicas[:], result)
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
		cluster.EXPECT().Resolve().Return(stringset.New(x, y)),
		cluster.EXPECT().Resolve().Return(stringset.New(y, z)),
	)

	clk := clock.NewMock()
	config := healthcheck.MonitorConfig{Interval: 1 * time.Second}
	monitor := healthcheck.NewMonitor(
		config,
		cluster,
		healthcheck.IdentityFilter{},
		healthcheck.WithClk(clk))
	runtime.Gosched()

	r := New(Config{}, monitor)

	d := core.DigestFixture()

	require.ElementsMatch([]string{x, y}, r.Locations(d))

	clk.Add(config.Interval + config.Interval/3)
	r.Refresh()

	require.ElementsMatch([]string{y, z}, r.Locations(d))
}

func TestRingNotifiesWatchersOnMembershipChanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cluster := mockhostlist.NewMockList(ctrl)

	watcher := mockhashring.NewMockWatcher(ctrl)

	x := "x:80"
	y := "y:80"
	z := "z:80"

	gomock.InOrder(
		// Called during initial refresh when ring is created.
		cluster.EXPECT().Resolve().Return(stringset.New(x, y)),
		watcher.EXPECT().Notify(stringset.New(x, y)),

		// Called on subsequent refresh.
		cluster.EXPECT().Resolve().Return(stringset.New(x, y, z)),
		watcher.EXPECT().Notify(stringset.New(x, y, z)),

		// No changes does not notify.
		cluster.EXPECT().Resolve().Return(stringset.New(x, y, z)),
	)

	clk := clock.NewMock()
	config := healthcheck.MonitorConfig{Interval: 1 * time.Second}
	monitor := healthcheck.NewMonitor(
		config,
		cluster,
		healthcheck.IdentityFilter{},
		healthcheck.WithClk(clk))
	runtime.Gosched()

	r := New(Config{}, monitor, WithWatcher(watcher))
	clk.Add(config.Interval + config.Interval/3)
	r.Refresh()
	clk.Add(config.Interval + config.Interval/3)
	r.Refresh()
}
