package healthcheck

import (
	"testing"

	"code.uber.internal/infra/kraken/utils/stringset"

	"github.com/stretchr/testify/require"
)

func TestStateHealthTransition(t *testing.T) {
	require := require.New(t)

	s := newState(FilterConfig{Fails: 3, Passes: 2})

	addr := "foo:80"

	s.passed(addr) // +1
	require.Empty(s.getHealthy())

	s.passed(addr) // +2 (healthy)
	require.Equal(stringset.New(addr), s.getHealthy())

	s.passed(addr) // +2 (noop)
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -1
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -2
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -3 (unhealthy)
	require.Empty(s.getHealthy())

	s.failed(addr) // -3 (noop)
	require.Empty(s.getHealthy())

	s.passed(addr) // +1
	require.Empty(s.getHealthy())

	s.passed(addr) // +2 (healthy)
	require.Equal(stringset.New(addr), s.getHealthy())

	s.passed(addr) // +2 (noop)
	require.Equal(stringset.New(addr), s.getHealthy())
}

func TestStateHealthTrendResets(t *testing.T) {
	require := require.New(t)

	s := newState(FilterConfig{Fails: 3, Passes: 2})

	addr := "foo:80"

	s.passed(addr) // +1
	require.Empty(s.getHealthy())

	s.passed(addr) // +2 (healthy)
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -1
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -2
	require.Equal(stringset.New(addr), s.getHealthy())

	s.passed(addr) // +1 (resets)
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -1
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -2
	require.Equal(stringset.New(addr), s.getHealthy())

	s.failed(addr) // -3 (unhealthy)
	require.Empty(s.getHealthy())
}

func TestStateSync(t *testing.T) {
	require := require.New(t)

	s := newState(FilterConfig{Fails: 1, Passes: 1})

	addr1 := "foo:80"
	addr2 := "bar:80"

	s.passed(addr1)
	s.passed(addr2)
	require.Equal(stringset.New(addr1, addr2), s.getHealthy())

	s.sync(stringset.New(addr1))

	require.Equal(stringset.New(addr1), s.getHealthy())
}
