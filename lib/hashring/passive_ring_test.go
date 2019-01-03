package hashring

import (
	"testing"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
)

func TestPassiveRingFailedAffectsRefreshFilter(t *testing.T) {
	require := require.New(t)

	r := NewPassive(
		Config{MaxReplica: 3},
		hostlist.Fixture(addrsFixture(10)...),
		healthcheck.NewPassiveFilter(healthcheck.PassiveFilterConfig{
				Fails: 3,
				FailTimeout: 5 * time.Second,
			}, clock.New()))

	d := core.DigestFixture()

	replicas := r.Locations(d)
	require.Len(replicas, 3)

	for i := 0; i < 4; i++ {
		r.Failed(replicas[0])
	}
	r.Refresh()

	result := r.Locations(d)
	require.Equal(replicas[1:], result)
}
