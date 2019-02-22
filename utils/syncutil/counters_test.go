package syncutil

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountersIncrement(t *testing.T) {
	require := require.New(t)

	c := NewCounters(10)

	wg := sync.WaitGroup{}
	for k := 0; k < 100; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			c.Increment(k % c.Len())
		}(k)
	}
	wg.Wait()

	for k := 0; k < c.Len(); k++ {
		require.Equal(10, c.Get(k))
	}
}

func TestCountersDecrement(t *testing.T) {
	require := require.New(t)

	c := NewCounters(10)
	for k := 0; k < c.Len(); k++ {
		c.Set(k, 10)
	}

	wg := sync.WaitGroup{}
	for k := 0; k < 100; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			c.Decrement(k % c.Len())
		}(k)
	}
	wg.Wait()

	for k := 0; k < c.Len(); k++ {
		require.Equal(0, c.Get(k))
	}
}

func TestCountersSet(t *testing.T) {
	require := require.New(t)

	c := NewCounters(10)

	wg := sync.WaitGroup{}
	for k := 0; k < 100; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			c.Set(k%c.Len(), -1)
		}(k)
	}
	wg.Wait()

	for k := 0; k < c.Len(); k++ {
		require.Equal(-1, c.Get(k))
	}
}
