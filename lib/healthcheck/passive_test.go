package healthcheck

import (
	"testing"
	"time"

	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/utils/stringset"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func TestPassiveUnhealthy(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()

	x := "x:80"
	y := "y:80"

	p := NewPassive(
		PassiveConfig{Fails: 3, FailTimeout: 10 * time.Second},
		clk,
		hostlist.Fixture(x, y))

	all := stringset.New(x, y)
	resolvedHealthy, resolvedAll := p.Resolve()
	require.Equal(all, resolvedHealthy)
	require.Equal(all, resolvedAll)

	for i := 0; i < 3; i++ {
		p.Failed(x)
	}

	healthy := stringset.New(y)
	resolvedHealthy, resolvedAll = p.Resolve()
	require.Equal(healthy, resolvedHealthy)
	require.Equal(all, resolvedAll)
}

func TestPassiveFailTimeout(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()

	x := "x:80"
	y := "y:80"

	p := NewPassive(
		PassiveConfig{Fails: 3, FailTimeout: 10 * time.Second},
		clk,
		hostlist.Fixture(x, y))

	p.Failed(x)
	p.Failed(x)

	clk.Add(11 * time.Second)

	p.Failed(x)

	all := stringset.New(x, y)
	resolvedHealthy, resolvedAll := p.Resolve()
	require.Equal(all, resolvedHealthy)
	require.Equal(all, resolvedAll)
}

func TestPassiveFailTimeoutAfterUnhealthy(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()

	x := "x:80"
	y := "y:80"

	p := NewPassive(
		PassiveConfig{Fails: 3, FailTimeout: 10 * time.Second},
		clk,
		hostlist.Fixture(x, y))

	for i := 0; i < 3; i++ {
		p.Failed(x)
	}

	all := stringset.New(x, y)
	healthy := stringset.New(y)
	resolvedHealthy, resolvedAll := p.Resolve()
	require.Equal(healthy, resolvedHealthy)
	require.Equal(all, resolvedAll)

	clk.Add(5 * time.Second)

	// Stil unhealthy...
	resolvedHealthy, resolvedAll = p.Resolve()
	require.Equal(healthy, resolvedHealthy)
	require.Equal(all, resolvedAll)

	clk.Add(6 * time.Second)

	// Timeout has now elapsed, host is healthy again.
	resolvedHealthy, resolvedAll = p.Resolve()
	require.Equal(all, resolvedHealthy)
	require.Equal(all, resolvedAll)
}

func TestPassiveIgnoresAllUnhealthy(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()

	x := "x:80"
	y := "y:80"

	p := NewPassive(
		PassiveConfig{Fails: 3, FailTimeout: 10 * time.Second},
		clk,
		hostlist.Fixture(x, y))

	for i := 0; i < 3; i++ {
		p.Failed(x)
		p.Failed(y)
	}

	all := stringset.New(x, y)
	resolvedHealthy, resolvedAll := p.Resolve()
	require.Equal(all, resolvedHealthy)
	require.Equal(all, resolvedAll)
}
