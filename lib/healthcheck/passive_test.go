package healthcheck

import (
	"testing"
	"time"

	"code.uber.internal/infra/kraken/lib/hostlist"
	"code.uber.internal/infra/kraken/utils/stringset"

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

	require.Equal(stringset.New(x, y), p.Resolve())

	for i := 0; i < 3; i++ {
		p.Failed(x)
	}

	require.Equal(stringset.New(y), p.Resolve())
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

	require.Equal(stringset.New(x, y), p.Resolve())
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

	require.Equal(stringset.New(y), p.Resolve())

	clk.Add(5 * time.Second)

	// Stil unhealthy...
	require.Equal(stringset.New(y), p.Resolve())

	clk.Add(6 * time.Second)

	// Timeout has now elapsed, host is healthy again.
	require.Equal(stringset.New(x, y), p.Resolve())
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

	require.Equal(stringset.New(x, y), p.Resolve())
}
