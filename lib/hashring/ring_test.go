// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package hashring

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
	mockhashring "github.com/uber/kraken/mocks/lib/hashring"
	mockhostlist "github.com/uber/kraken/mocks/lib/hostlist"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/stringset"
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

			r := New(
				Config{MaxReplica: test.maxReplica},
				hostlist.Fixture(addrs...),
				healthcheck.IdentityFilter{},
				tally.NoopScope)

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

	r := New(
		Config{MaxReplica: 3},
		hostlist.Fixture(addrsFixture(10)...),
		filter,
		tally.NoopScope)

	d := core.DigestFixture()

	replicas := r.Locations(d)
	require.Len(replicas, 3)

	filter.Unhealthy.Add(replicas[0])
	r.Refresh()

	result := r.Locations(d)
	require.Equal(replicas[1:], result)
}

func TestRingLocationsReturnsNextHealthyHostWhenReplicaSetUnhealthy(t *testing.T) {
	require := require.New(t)

	filter := healthcheck.NewManualFilter()

	r := New(
		Config{MaxReplica: 3},
		hostlist.Fixture(addrsFixture(10)...),
		filter,
		tally.NoopScope)

	d := core.DigestFixture()

	replicas := r.Locations(d)
	require.Len(replicas, 3)

	// Mark all the original replicas as unhealthy.
	for _, addr := range replicas {
		filter.Unhealthy.Add(addr)
	}
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

	filter := healthcheck.NewBinaryFilter()

	r := New(
		Config{MaxReplica: 3},
		hostlist.Fixture(addrsFixture(10)...),
		filter,
		tally.NoopScope)

	d := core.DigestFixture()

	replicas := r.Locations(d)
	require.Len(replicas, 3)

	filter.Healthy = false
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

	r := New(Config{}, hostlist.Fixture(x, y), healthcheck.IdentityFilter{}, tally.NoopScope)

	require.True(r.Contains(x))
	require.True(r.Contains(y))
	require.False(r.Contains(z))
}

func TestRingWaitForContains(t *testing.T) {
	tests := map[string]struct {
		addrs    []string
		giveAddr string
		wantErr  string
	}{
		"instant success": {
			addrs:    []string{"x:80", "y:80"},
			giveAddr: "y:80",
		},
		"timeout": {
			addrs:    []string{"x:80"},
			giveAddr: "y:80",
			wantErr:  "timed out waiting for membership: address y:80 not found in ring",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			require := require.New(t)

			r := New(
				Config{MembershipWaitTimeout: 100 * time.Millisecond},
				hostlist.Fixture(tt.addrs...),
				healthcheck.IdentityFilter{},
				tally.NoopScope)

			err := r.WaitForContains(tt.giveAddr)

			if tt.wantErr != "" {
				require.EqualError(err, tt.wantErr)
			} else {
				require.NoError(err)
			}
		})
	}
}

func TestRingWaitForContainsDelayedSuccess(t *testing.T) {
	require := require.New(t)

	ctrl := gomock.NewController(t)
	cluster := mockhostlist.NewMockList(ctrl)
	cluster.EXPECT().Resolve().Return(stringset.New("x:80")).Times(1)
	cluster.EXPECT().Resolve().Return(stringset.New("x:80", "y:80")).AnyTimes()

	r := New(Config{}, cluster, healthcheck.IdentityFilter{}, tally.NoopScope)
	require.False(r.Contains("y:80"))

	// Simulate a DNS refresh that adds the new address.
	time.AfterFunc(100*time.Millisecond, func() {
		r.Refresh()
	})

	err := r.WaitForContains("y:80")

	require.NoError(err)
}

func TestRingMembers(t *testing.T) {
	require := require.New(t)

	x := "x:80"
	y := "y:80"
	z := "z:80"

	r := New(Config{}, hostlist.Fixture(x, y, z), healthcheck.IdentityFilter{}, tally.NoopScope)

	require.True(stringset.Equal(stringset.New(x, y, z), r.Members()))
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

	r := New(
		Config{RefreshInterval: time.Second},
		cluster,
		healthcheck.IdentityFilter{},
		tally.NoopScope)

	stop := make(chan struct{})
	defer close(stop)
	go r.Monitor(stop)

	d := core.DigestFixture()

	require.Equal([]string{x}, r.Locations(d))

	// Monitor should refresh the ring.
	time.Sleep(1250 * time.Millisecond)

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
		cluster.EXPECT().Resolve().Return(stringset.New(x, y)),
		cluster.EXPECT().Resolve().Return(stringset.New(y, z)),
	)

	r := New(Config{}, cluster, healthcheck.IdentityFilter{}, tally.NoopScope)

	d := core.DigestFixture()

	require.ElementsMatch([]string{x, y}, r.Locations(d))

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

	r := New(Config{}, cluster, healthcheck.IdentityFilter{}, tally.NoopScope, WithWatcher(watcher))
	r.Refresh()
	r.Refresh()
}
