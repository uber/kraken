package healthcheck

import (
	"testing"
	"time"

	"github.com/uber/kraken/utils/stringset"

	"github.com/andres-erbsen/clock"
	"github.com/stretchr/testify/require"
)

func TestPassiveFilterUnhealthy(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()

	f := NewPassiveFilter(
		PassiveFilterConfig{Fails: 3, FailTimeout: 10 * time.Second},
		clk)

	x := "x:80"
	y := "y:80"
	s := stringset.New(x, y)

	require.Equal(stringset.New(x, y), f.Run(s))

	for i := 0; i < 3; i++ {
		f.Failed(x)
	}

	require.Equal(stringset.New(y), f.Run(s))
}

func TestPassiveFilterFailTimeout(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()

	f := NewPassiveFilter(
		PassiveFilterConfig{Fails: 3, FailTimeout: 10 * time.Second},
		clk)

	x := "x:80"
	y := "y:80"
	s := stringset.New(x, y)

	f.Failed(x)
	f.Failed(x)

	clk.Add(11 * time.Second)

	f.Failed(x)

	require.Equal(stringset.New(x, y), f.Run(s))
}

func TestPassiveFilterFailTimeoutAfterUnhealthy(t *testing.T) {
	require := require.New(t)

	clk := clock.NewMock()

	f := NewPassiveFilter(
		PassiveFilterConfig{Fails: 3, FailTimeout: 10 * time.Second},
		clk)

	x := "x:80"
	y := "y:80"
	s := stringset.New(x, y)

	for i := 0; i < 3; i++ {
		f.Failed(x)
	}

	require.Equal(stringset.New(y), f.Run(s))

	clk.Add(5 * time.Second)

	// Stil unhealthy...
	require.Equal(stringset.New(y), f.Run(s))

	clk.Add(6 * time.Second)

	// Timeout has now elapsed, host is healthy again.
	require.Equal(stringset.New(x, y), f.Run(s))
}
